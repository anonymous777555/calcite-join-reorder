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
import org.apache.calcite.rel.core.BestMatch;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Nullify;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This {@link RelOptRule} identifies the fact that {@link BestMatch} and {@link Nullify}
 * are usually generated together. Thus, it pulls up them together.
 */
public class BestMatchNullifyPullUpRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final BestMatchNullifyPullUpRule LEFT_CHILD = new BestMatchNullifyPullUpRule(
      operand(Join.class,
          operand(BestMatch.class, operand(Nullify.class, any())),
          operand(RelSubset.class, any())),
      "BestMatchNullifyPullUp(Left)", true);
  public static final BestMatchNullifyPullUpRule RIGHT_CHILD = new BestMatchNullifyPullUpRule(
      operand(Join.class,
          operand(RelSubset.class, any()),
          operand(BestMatch.class, operand(Nullify.class, any()))),
      "BestMatchNullifyPullUp(Right)", false);

  // Whether nullify is left child of join in the original expression.
  private final boolean isLeftChild;

  //~ Constructors -----------------------------------------------------------

  public BestMatchNullifyPullUpRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory, boolean isLeftChild) {
    super(operand, relBuilderFactory, description);
    this.isLeftChild = isLeftChild;
  }

  public BestMatchNullifyPullUpRule(RelOptRuleOperand operand,
      String description, boolean isLeftChild) {
    this(operand, description, RelFactories.LOGICAL_BUILDER, isLeftChild);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    final Join join = call.rel(0);
    final BestMatch bestMatch = call.rel(isLeftChild ? 1 : 2);
    final Nullify nullify = call.rel(isLeftChild ? 2 : 3);
    final RelSubset otherChild = call.rel(isLeftChild ? 3 : 1);

    // Replaces the variables in the attributes & predicate later.
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final int leftSize = call.rel(1).getRowType().getFieldCount();
    final VariableReplacer replacer = new VariableReplacer(rexBuilder, leftSize);

    // Checks whether the join predicate refers to old nullification list.
    final Set<RexInputRef> attributes = replacer.replace(nullify.getAttributes(), isLeftChild);
    final RexNode joinPredicate = join.getCondition();
    final boolean isReferring = RelOptCustomUtil.isReferringTo(joinPredicate, attributes);

    // Constructs the new join.
    final JoinRelType oldJoinType = join.getJoinType();
    final boolean needChangeJoinType = oldJoinType == JoinRelType.ANTI && isReferring;
    final Join newJoin = join.copy(
        join.getTraitSet(),
        joinPredicate,
        isLeftChild ? nullify.getInput() : otherChild,
        isLeftChild ? otherChild : nullify.getInput(),
        needChangeJoinType ? JoinRelType.LEFT : oldJoinType,
        join.isSemiJoinDone());

    // Prepares for the new nullification operator.
    RexNode nullifyPredicate = nullify.getPredicate();
    if (!isLeftChild) {
      nullifyPredicate = replacer.replaceFromBottomToTop(nullifyPredicate);
    }
    List<RexInputRef> allFields = newJoin.getRowType().getFieldList().stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());
    List<RexInputRef> leftFields = allFields.subList(0, leftSize);
    List<RexInputRef> rightFields = allFields.subList(leftSize, allFields.size());

    // Builds the new expression.
    final RelBuilder builder = call.builder();
    RelNode transformedNode = newJoin;
    switch (oldJoinType) {
    case INNER:
      if (isReferring) {
        attributes.addAll(allFields);
      }
      transformedNode = builder.push(newJoin)
          .nullify(nullifyPredicate, attributes).bestMatch().build();
      break;
    case LEFT:
      if (isReferring) {
        attributes.addAll(rightFields);
      }
      transformedNode = builder.push(newJoin)
          .nullify(nullifyPredicate, attributes).bestMatch().build();
      break;
    case FULL:
      if (isReferring) {
        attributes.addAll(rightFields);
        transformedNode = builder.push(newJoin)
            .nullifyStar(nullifyPredicate, leftFields, attributes)
            .bestMatch().build();
      } else {
        transformedNode = builder.push(newJoin)
            .nullify(nullifyPredicate, attributes).bestMatch().build();
      }
      break;
    case ANTI:
      if (isReferring) {
        attributes.addAll(rightFields);
        transformedNode = builder.push(newJoin)
            .nullify(nullifyPredicate, attributes).bestMatch()
            .gamma(rightFields).project(leftFields)
            .build();
      } else if (isLeftChild) {
        transformedNode = builder.push(newJoin)
            .nullify(nullifyPredicate, attributes).bestMatch().build();
      }
      break;
    default:
      LOGGER.debug("Unable to pull up best-match + nullify for join type " + oldJoinType);
      return;
    }
    call.transformTo(transformedNode);

    // Registers this transformation as a pair of equivalent queries.
    final Join beforeNode = join.copy(
        join.getTraitSet(),
        joinPredicate,
        isLeftChild ? bestMatch : otherChild,
        isLeftChild ? otherChild : bestMatch,
        oldJoinType,
        join.isSemiJoinDone());
    RelOptCustomUtil.registerTransformationPair(beforeNode, transformedNode, getClass().getName());
  }

  /**
   * An utility inner class for pulling up across a join operator.
   */
  private static class VariableReplacer extends AssocReverseVariableReplacer {
    VariableReplacer(final RexBuilder rexBuilder, final int left) {
      super(rexBuilder, left);
    }

    Set<RexInputRef> replace(final List<RexNode> attributes, final boolean isLeftChild) {
      return attributes.stream()
          .map(attr -> (RexInputRef) (isLeftChild ? attr : replaceFromBottomToTop(attr)))
          .collect(Collectors.toSet());
    }
  }
}

// End BestMatchNullifyPullUpRule.java
