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

import org.apache.calcite.plan.RelOptCustomUtil;
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
 * AsscomLeftLeftRule applies limited r-asscom property on left join and left join.
 *
 * Rule 19.
 */
public class AsscomLeftLeftRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final AsscomLeftLeftRule INSTANCE = new AsscomLeftLeftRule(
      operand(Join.class,
          operand(RelSubset.class, any()),
          operand(Join.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public AsscomLeftLeftRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public AsscomLeftLeftRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    // Gets the two original join operators.
    final Join topLeftJoin = call.rel(0);
    final Join bottomLeftJoin = call.rel(2);

    // Makes sure the join types match the rule.
    if (topLeftJoin.getJoinType() != JoinRelType.LEFT) {
      LOGGER.debug("The top join is not an left outer join.");
      return;
    } else if (bottomLeftJoin.getJoinType() != JoinRelType.LEFT) {
      LOGGER.debug("The bottom join is not a left outer join.");
      return;
    }

    // Makes sure the join condition is referring to the correct set of fields.
    final int topLeftJoinLeft = topLeftJoin.getLeft().getRowType().getFieldCount();
    final int bottomLeftJoinLeft = bottomLeftJoin.getLeft().getRowType().getFieldCount();
    final int bottomLeftJoinRight = bottomLeftJoin.getRight().getRowType().getFieldCount();
    final List<RelDataTypeField> fields = topLeftJoin.getRowType().getFieldList();
    if (RelOptCustomUtil.isReferringTo(topLeftJoin.getCondition(),
        fields.subList(topLeftJoinLeft, fields.size() - bottomLeftJoinRight))) {
      LOGGER.debug("Not a subset of attributes.");
      return;
    }

    // Replaces the variables in the predicates later.
    final RexBuilder rexBuilder = topLeftJoin.getCluster().getRexBuilder();
    final AsscomVariableReplacer replacer = new AsscomVariableReplacer(
        rexBuilder, topLeftJoinLeft, bottomLeftJoinLeft, bottomLeftJoinRight);

    // The new operators.
    final Join newBottomLeftJoin = topLeftJoin.copy(
        topLeftJoin.getTraitSet(),
        replacer.replaceFromTopToBottom(topLeftJoin.getCondition()),
        topLeftJoin.getLeft(),
        bottomLeftJoin.getRight(),
        JoinRelType.LEFT,
        topLeftJoin.isSemiJoinDone());
    final Join newTopLeftJoin = bottomLeftJoin.copy(
        bottomLeftJoin.getTraitSet(),
        replacer.replaceFromBottomToTop(bottomLeftJoin.getCondition()),
        newBottomLeftJoin,
        bottomLeftJoin.getLeft(),
        JoinRelType.LEFT,
        bottomLeftJoin.isSemiJoinDone());

    // Determines the nullification attribute.
    final List<RelDataTypeField> nullifyFieldList = newTopLeftJoin.getRowType()
        .getFieldList().subList(topLeftJoinLeft, topLeftJoinLeft + bottomLeftJoinRight);
    final List<RexNode> nullificationList = nullifyFieldList.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    // Builds the transformed relational tree.
    final RelNode transformedNode = call.builder().push(newTopLeftJoin)
        .nullify(newTopLeftJoin.getCondition(), nullificationList).bestMatch().build();
    final RelNode projectedNode = call.builder().push(transformedNode)
        .project(replacer.getProjectFields(transformedNode)).build();
    call.transformTo(projectedNode);

    // Registers this transformation as a pair of equivalent queries.
    final RelNode beforeNode = topLeftJoin.copy(
        topLeftJoin.getTraitSet(),
        topLeftJoin.getCondition(),
        topLeftJoin.getLeft(),
        bottomLeftJoin,
        topLeftJoin.getJoinType(),
        topLeftJoin.isSemiJoinDone());
    RelOptCustomUtil.registerTransformationPair(beforeNode, projectedNode, getClass().getName());
  }
}

// End AsscomLeftLeftRule.java
