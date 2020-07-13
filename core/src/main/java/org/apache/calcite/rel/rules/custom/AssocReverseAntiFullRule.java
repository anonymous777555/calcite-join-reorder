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
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * AssocReverseAntiFullRule applies limited associativity on anti join and full join.
 *
 * Rule 4.
 */
public class AssocReverseAntiFullRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final AssocReverseAntiFullRule INSTANCE = new AssocReverseAntiFullRule(
      operand(Join.class,
          operand(RelSubset.class, any()),
          operand(Join.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public AssocReverseAntiFullRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public AssocReverseAntiFullRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    // Gets the two original join operators.
    final Join topJoin = call.rel(0);
    final Join bottomJoin = call.rel(2);

    // Checks whether the join type is valid for this rule.
    final JoinRelType topJoinType = topJoin.getJoinType();
    final JoinRelType bottomJoinType = bottomJoin.getJoinType();
    if (topJoinType != JoinRelType.FULL) {
      LOGGER.debug("The top join type is not applicable for " + getClass().getName());
      return;
    } else if (bottomJoinType != JoinRelType.ANTI) {
      LOGGER.debug("The bottom join type is not applicable for " + getClass().getName());
      return;
    }

    // Replaces the variables in the predicates later.
    final int topJoinLeft = topJoin.getLeft().getRowType().getFieldCount();
    final int bottomJoinLeft = bottomJoin.getLeft().getRowType().getFieldCount();
    final RexBuilder rexBuilder = topJoin.getCluster().getRexBuilder();
    final AssocReverseVariableReplacer replacer
        = new AssocReverseVariableReplacer(rexBuilder, topJoinLeft);

    // The new operators.
    final Join newBottomJoin = topJoin.copy(
        topJoin.getTraitSet(),
        topJoin.getCondition(),
        topJoin.getLeft(),
        bottomJoin.getLeft(),
        JoinRelType.FULL,
        topJoin.isSemiJoinDone());
    final Join newTopJoin = bottomJoin.copy(
        bottomJoin.getTraitSet(),
        replacer.replaceFromBottomToTop(bottomJoin.getCondition()),
        newBottomJoin,
        bottomJoin.getRight(),
        JoinRelType.LEFT,
        bottomJoin.isSemiJoinDone());

    // Determines the nullification & projection attributes.
    final int total = newTopJoin.getRowType().getFieldCount();
    final List<RelDataTypeField> topLeftFields = newTopJoin.getRowType().getFieldList()
        .subList(0, topJoinLeft);
    final List<RexNode> topLeftList = topLeftFields.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());
    final List<RelDataTypeField> bottomFields = newTopJoin.getRowType().getFieldList()
        .subList(topJoinLeft, total);
    final List<RexNode> bottomList = bottomFields.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());
    final List<RelDataTypeField> projectFields = newTopJoin.getRowType().getFieldList()
        .subList(0, topJoinLeft + bottomJoinLeft);
    final List<RexNode> projectList = projectFields.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    // Builds the transformed relational tree.
    final RelNode transformedNode = call.builder().push(newTopJoin)
        .gammaStar(bottomList, topLeftList).project(projectList)
        .build();
    call.transformTo(transformedNode);
  }
}

// End AssocReverseAntiFullRule.java
