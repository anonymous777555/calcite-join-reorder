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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * AssocReverseInnerFullRule applies limited associativity on inner join and full join.
 *
 * Rule 2.
 */
public class AssocReverseInnerFullRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final AssocReverseInnerFullRule INSTANCE = new AssocReverseInnerFullRule(
      operand(Join.class,
          operand(RelSubset.class, any()),
          operand(Join.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public AssocReverseInnerFullRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public AssocReverseInnerFullRule(RelOptRuleOperand operand, String description) {
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
    } else if (bottomJoinType != JoinRelType.INNER) {
      LOGGER.debug("The bottom join type is not applicable for " + getClass().getName());
      return;
    }

    // Makes sure the top join predicate is referring to the correct set of fields.
    final int topJoinLeft = topJoin.getLeft().getRowType().getFieldCount();
    final int bottomJoinRight = bottomJoin.getRight().getRowType().getFieldCount();
    final int total = topJoin.getRowType().getFieldCount();
    if (RelOptCustomUtil.isReferringTo(topJoin.getCondition(),
        topJoin.getRowType().getFieldList().subList(total - bottomJoinRight, total))) {
      LOGGER.debug("Not a subset of attributes.");
      return;
    }

    // Replaces the variables in the predicates later.
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
    final RelBuilder builder = call.builder();
    final List<RelDataTypeField> bottomRightFields = newTopJoin.getRowType().getFieldList()
        .subList(total - bottomJoinRight, total);
    final List<RexNode> checkNotNullList = bottomRightFields.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .map(field -> builder.call(SqlStdOperatorTable.IS_NOT_NULL, field))
        .collect(Collectors.toList());
    final RexNode nullifyPredicate = builder.or(checkNotNullList);

    final List<RelDataTypeField> bottomLeftFields = newTopJoin.getRowType().getFieldList()
        .subList(topJoinLeft, total - bottomJoinRight);
    final List<RexNode> bottomLeftList = bottomLeftFields.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    // Builds the transformed relational tree.
    final RelNode transformedNode = builder.push(newTopJoin)
        .nullify(nullifyPredicate, bottomLeftList).bestMatch()
        .build();
    call.transformTo(transformedNode);
  }
}

// End AssocReverseInnerFullRule.java
