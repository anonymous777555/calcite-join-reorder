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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;

/**
 * AssocFullInnerRule applies limited associativity on full join and inner join.
 *
 * Rule 1.
 */
public class AssocFullInnerRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final AssocFullInnerRule INSTANCE = new AssocFullInnerRule(
      operand(Join.class,
          operand(Join.class, any()),
          operand(RelSubset.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public AssocFullInnerRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public AssocFullInnerRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    // Gets the two original join operators.
    final Join topJoin = call.rel(0);
    final Join bottomJoin = call.rel(1);

    // Checks whether the join type is valid for this rule.
    final JoinRelType topJoinType = topJoin.getJoinType();
    final JoinRelType bottomJoinType = bottomJoin.getJoinType();
    if (topJoinType != JoinRelType.INNER) {
      LOGGER.debug("The top join type is not applicable for " + getClass().getName());
      return;
    } else if (bottomJoinType != JoinRelType.FULL) {
      LOGGER.debug("The bottom join type is not applicable for " + getClass().getName());
      return;
    }

    // Makes sure the top join predicate is referring to the correct set of fields.
    final int bottomJoinLeft = bottomJoin.getLeft().getRowType().getFieldCount();
    final int bottomJoinRight = bottomJoin.getRight().getRowType().getFieldCount();
    final int topJoinRight = topJoin.getRight().getRowType().getFieldCount();
    if (RelOptCustomUtil.isReferringTo(topJoin.getCondition(),
        topJoin.getRowType().getFieldList().subList(0, bottomJoinLeft))) {
      LOGGER.debug("Not a subset of attributes.");
      return;
    }

    // Replaces the variables in the predicates later.
    final RexBuilder rexBuilder = topJoin.getCluster().getRexBuilder();
    final VariableReplacer replacer
        = new VariableReplacer(rexBuilder, bottomJoinLeft, bottomJoinRight, topJoinRight);

    // The new operators.
    final Join newBottomJoin = topJoin.copy(
        topJoin.getTraitSet(),
        replacer.replaceFromTopToBottom(topJoin.getCondition()),
        bottomJoin.getRight(),
        topJoin.getRight(),
        JoinRelType.INNER,
        topJoin.isSemiJoinDone());
    final Join newTopJoin = bottomJoin.copy(
        bottomJoin.getTraitSet(),
        replacer.replaceFromBottomToTop(bottomJoin.getCondition()),
        newBottomJoin,
        bottomJoin.getLeft(),
        JoinRelType.LEFT,
        bottomJoin.isSemiJoinDone());

    // Builds the transformed relational tree.
    call.transformTo(newTopJoin);
  }

  /**
   * A utility inner to be used when commutativity is applied after associativity.
   */
  private static class VariableReplacer extends AssocVariableReplacer {
    private final int bottomRight;
    private final int topRight;

    VariableReplacer(final RexBuilder rexBuilder,
        final int bottomLeft, final int bottomRight, final int topRight) {
      super(rexBuilder, bottomLeft);
      this.bottomRight = bottomRight;
      this.topRight = topRight;
    }

    RexNode replaceFromBottomToTop(final RexNode rex) {
      if (rex instanceof RexCall) {
        final RexCall call = (RexCall) rex;

        // Converts each operand in the predicate.
        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        call.operands.forEach(operand -> builder.add(replaceFromBottomToTop(operand)));

        // Re-builds the predicate.
        return call.clone(call.getType(), builder.build());
      } else if (rex instanceof RexInputRef) {
        final RexInputRef var = (RexInputRef) rex;

        // Computes its index after transformation.
        int newIndex;
        if (var.getIndex() < bottomLeft) {
          newIndex = var.getIndex() + bottomRight + topRight;
        } else {
          newIndex = var.getIndex() - bottomLeft;
        }

        // Re-builds the attribute.
        return rexBuilder.makeInputRef(var.getType(), newIndex);
      } else {
        return rex;
      }
    }
  }
}

// End AssocFullInnerRule.java
