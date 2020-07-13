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
import org.apache.calcite.rel.core.NullifyStar;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This {@link RelOptRule} identifies the fact that {@link BestMatch} and {@link NullifyStar}
 * are usually generated together. Thus, it pulls up them together.
 */
public class BestMatchNullifyStarPullUpRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final BestMatchNullifyStarPullUpRule LEFT_CHILD
      = new BestMatchNullifyStarPullUpRule(
          operand(Join.class,
              operand(BestMatch.class, operand(NullifyStar.class, any())),
              operand(RelSubset.class, any())),
      "BestMatchNullifyStarPullUp(Left)", true);
  public static final BestMatchNullifyStarPullUpRule RIGHT_CHILD
      = new BestMatchNullifyStarPullUpRule(
          operand(Join.class,
              operand(RelSubset.class, any()),
              operand(BestMatch.class, operand(NullifyStar.class, any()))),
      "BestMatchNullifyStarPullUp(Right)", false);

  // Whether nullifyStar is left child of join in the original expression.
  private final boolean isLeftChild;

  //~ Constructors -----------------------------------------------------------

  public BestMatchNullifyStarPullUpRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory, boolean isLeftChild) {
    super(operand, relBuilderFactory, description);
    this.isLeftChild = isLeftChild;
  }

  public BestMatchNullifyStarPullUpRule(RelOptRuleOperand operand,
      String description, boolean isLeftChild) {
    this(operand, description, RelFactories.LOGICAL_BUILDER, isLeftChild);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    final Join join = call.rel(0);
    final NullifyStar nullifyStar = call.rel(isLeftChild ? 2 : 3);
    final RelSubset otherChild = call.rel(isLeftChild ? 3 : 1);

    // Constructs the new join.
    final JoinRelType joinType = join.getJoinType();
    final RexNode joinPredicate = join.getCondition();
    final Join newJoin = join.copy(
        join.getTraitSet(),
        joinPredicate,
        isLeftChild ? nullifyStar.getInput() : otherChild,
        isLeftChild ? otherChild : nullifyStar.getInput(),
        joinType,
        join.isSemiJoinDone());

    // Replaces the variables in the attributes & predicate later.
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final int leftSize = newJoin.getLeft().getRowType().getFieldCount();
    final VariableReplacer replacer = new VariableReplacer(rexBuilder, leftSize);

    // Prepares for the new nullification operator.
    RexNode nullifyStarPredicate = nullifyStar.getPredicate();
    if (!isLeftChild) {
      nullifyStarPredicate = replacer.replaceFromBottomToTop(nullifyStarPredicate);
    }
    List<RexInputRef> allFields = newJoin.getRowType().getFieldList().stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());
    List<RexInputRef> leftFields = allFields.subList(0, leftSize);
    List<RexInputRef> rightFields = allFields.subList(leftSize, allFields.size());

    // Checks whether the join predicate refers to old nullification list.
    final Set<RexInputRef> attributesA = replacer.replace(nullifyStar.getAttributesA());
    final Set<RexInputRef> attributesB = replacer.replace(nullifyStar.getAttributesB());
    final Set<RexInputRef> attributes = new HashSet<>();
    attributes.addAll(attributesA);
    attributes.addAll(attributesB);
    final boolean isReferring = RelOptCustomUtil.isReferringTo(joinPredicate, attributes);

    // Builds the new expression.
    final RelBuilder builder = call.builder();
    RelNode transformedNode = newJoin;
    switch (joinType) {
    case INNER:
      builder.push(newJoin).nullifyStar(nullifyStarPredicate, attributesA, attributesB);
      if (isReferring) {
        builder.nullify(joinPredicate, allFields);
      }
      transformedNode = builder.bestMatch().build();
      break;
    case LEFT:
      builder.push(newJoin).nullifyStar(nullifyStarPredicate, attributesA, attributesB);
      if (isReferring) {
        builder.nullify(joinPredicate, rightFields);
      }
      transformedNode = builder.bestMatch().build();
      break;
    case FULL:
      builder.push(newJoin).nullifyStar(nullifyStarPredicate, attributesA, attributesB);
      if (isReferring) {
        builder.bestMatch().nullifyStar(joinPredicate, leftFields, rightFields);
      }
      transformedNode = builder.bestMatch().build();
      break;
    case ANTI:
      if (isReferring) {
        transformedNode = builder.push(newJoin)
            .nullifyStar(nullifyStarPredicate, attributesA, attributesB)
            .nullify(joinPredicate, rightFields).bestMatch()
            .gamma(rightFields).project(leftFields).build();
      } else if (isLeftChild) {
        transformedNode = builder.push(newJoin)
            .nullifyStar(nullifyStarPredicate, attributesA, attributesB)
            .bestMatch().build();
      }
      break;
    default:
      LOGGER.debug("Unable to pull up best-match + nullifyStar for join type " + joinType);
      return;
    }
    call.transformTo(transformedNode);

    // Registers this transformation as a pair of equivalent queries.
    final Join beforeNode = join.copy(
        join.getTraitSet(),
        joinPredicate,
        isLeftChild ? nullifyStar : otherChild,
        isLeftChild ? otherChild : nullifyStar,
        joinType,
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

    Set<RexInputRef> replace(List<RexNode> attributes) {
      return attributes.stream()
          .map(attr -> (RexInputRef) replaceFromBottomToTop(attr))
          .collect(Collectors.toSet());
    }
  }
}

// End BestMatchNullifyStarPullUpRule.java
