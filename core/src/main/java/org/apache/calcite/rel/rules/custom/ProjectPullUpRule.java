/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules.custom;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This {@link RelOptRule} pulls up a {@link Project} over a {@link Join}.
 */
public class ProjectPullUpRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** Instance of the current rule. */
  public static final ProjectPullUpRule LEFT_CHILD = new ProjectPullUpRule(
      operand(Join.class,
          operand(Project.class, any()),
          operand(RelSubset.class, any())),
      "ProjectPullUp(Left)", true);
  public static final ProjectPullUpRule RIGHT_CHILD = new ProjectPullUpRule(
      operand(Join.class,
          operand(RelSubset.class, any()),
          operand(Project.class, any())),
      "ProjectPullUp(Right)", false);

  // Whether nullify is left child of join in the original expression.
  private final boolean isLeftChild;

  //~ Constructors -----------------------------------------------------------

  public ProjectPullUpRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory, boolean isLeftChild) {
    super(operand, relBuilderFactory, description);
    this.isLeftChild = isLeftChild;
  }

  public ProjectPullUpRule(RelOptRuleOperand operand,
      String description, boolean isLeftChild) {
    this(operand, description, RelFactories.LOGICAL_BUILDER, isLeftChild);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    final Join join = call.rel(0);
    final Project project = call.rel(isLeftChild ? 1 : 2);
    final RelSubset otherChild = call.rel(isLeftChild ? 2 : 1);

    // Replaces the variables in the attributes & predicate later.
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelNode newLeft = isLeftChild ? project.getInput() : otherChild;
    final int left = newLeft.getRowType().getFieldCount();
    final List<RexNode> projects = new ArrayList<>(project.getProjects());
    final VariableReplacer replacer
        = new VariableReplacer(rexBuilder, left, isLeftChild, projects);

    // Constructs the new join.
    final JoinRelType joinType = join.getJoinType();
    final Join newJoin = join.copy(
        join.getTraitSet(),
        replacer.replaceJoin(join.getCondition()),
        newLeft,
        isLeftChild ? otherChild : project.getInput(),
        joinType,
        join.isSemiJoinDone());
    final List<RexNode> fields = newJoin.getRowType().getFieldList().stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    // Builds the new project list.
    final RelBuilder builder = call.builder().push(newJoin);
    RelNode transformedNode = newJoin;
    if (isLeftChild) {
      // Merges with the right side.
      if (joinType.projectsRight()) {
        projects.addAll(fields.subList(left, fields.size()));
      }

      // Pulls up the project.
      transformedNode = builder.project(projects).build();
    } else if (joinType.projectsRight()) {
      // Updates the indices & merges with the left side.
      final List<RexNode> newProjects = replacer.replaceProject(projects);
      newProjects.addAll(fields.subList(0, left));

      // Pulls up the projects.
      transformedNode = builder.project(newProjects).build();
    }

    // Builds the new expression.
    call.transformTo(transformedNode);
  }

  /**
   * An utility inner class for pulling up across a join operator.
   */
  private static class VariableReplacer extends AssocReverseVariableReplacer {
    private final boolean isLeft;
    private final List<RexNode> projects;

    VariableReplacer(final RexBuilder rexBuilder, final int left, final boolean isLeft,
        final List<RexNode> projects) {
      super(rexBuilder, left);
      this.isLeft = isLeft;
      this.projects = projects;
    }

    List<RexNode> replaceProject(final List<RexNode> attributes) {
      return attributes.stream().map(this::replaceFromBottomToTop).collect(Collectors.toList());
    }

    RexNode replaceJoin(final RexNode rex) {
      if (rex instanceof RexCall) {
        final RexCall call = (RexCall) rex;

        // Converts each operand in the predicate.
        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        call.operands.forEach(operand -> builder.add(replaceFromBottomToTop(operand)));

        // Re-builds the predicate.
        return call.clone(call.getType(), builder.build());
      } else if (rex instanceof RexInputRef) {
        final RexInputRef var = (RexInputRef) rex;

        // Computes the new index.
        int newIndex = var.getIndex();
        if (isLeft) {
          if (newIndex < projects.size()) {
            final RexInputRef project = (RexInputRef) projects.get(newIndex);
            newIndex = project.getIndex();
          } else {
            newIndex += topLeft - projects.size();
          }
        } else if (newIndex >= projects.size()) {
          final RexInputRef project = (RexInputRef) projects.get(newIndex - topLeft);
          newIndex = project.getIndex() + topLeft;
        }

        // Re-builds the attribute.
        return rexBuilder.makeInputRef(var.getType(), newIndex);
      } else {
        return rex;
      }
    }
  }
}

// End ProjectPullUpRule.java
