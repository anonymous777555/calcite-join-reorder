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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;

import java.util.Set;

/**
 * AsscomGeneralLeftRule applies all valid transformations based on l-asscom. It
 * implements the rules in TBA.
 */
public class AsscomGeneralLeftRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final AsscomGeneralLeftRule INSTANCE = new AsscomGeneralLeftRule(
      operand(Join.class,
          operand(Join.class, any()),
          operand(RelSubset.class, any())), null);

  // All valid pairs of join types according to TBA.
  private static final Set<Pair<JoinRelType, JoinRelType>> VALID_PAIRS = ImmutableSet.of(
      Pair.of(JoinRelType.INNER, JoinRelType.INNER),
      Pair.of(JoinRelType.INNER, JoinRelType.LEFT),
      Pair.of(JoinRelType.INNER, JoinRelType.SEMI),
      Pair.of(JoinRelType.INNER, JoinRelType.ANTI),
      Pair.of(JoinRelType.LEFT, JoinRelType.INNER),
      Pair.of(JoinRelType.LEFT, JoinRelType.LEFT),
      Pair.of(JoinRelType.LEFT, JoinRelType.SEMI),
      Pair.of(JoinRelType.LEFT, JoinRelType.ANTI),
      Pair.of(JoinRelType.LEFT, JoinRelType.FULL),
      Pair.of(JoinRelType.FULL, JoinRelType.LEFT),
      Pair.of(JoinRelType.FULL, JoinRelType.FULL),
      Pair.of(JoinRelType.SEMI, JoinRelType.INNER),
      Pair.of(JoinRelType.SEMI, JoinRelType.LEFT),
      Pair.of(JoinRelType.SEMI, JoinRelType.SEMI),
      Pair.of(JoinRelType.SEMI, JoinRelType.ANTI),
      Pair.of(JoinRelType.ANTI, JoinRelType.INNER),
      Pair.of(JoinRelType.ANTI, JoinRelType.LEFT),
      Pair.of(JoinRelType.ANTI, JoinRelType.SEMI),
      Pair.of(JoinRelType.ANTI, JoinRelType.ANTI)
  );

  //~ Constructors -----------------------------------------------------------

  public AsscomGeneralLeftRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public AsscomGeneralLeftRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Performs the transformation depicted as follows:
   *
   *           topJoin                  new top join
   *           /     \                  /          \
   *      bottomJoin  C    to   new bottom join    B
   *       /    \                 /        \
   *      A      B               A         C
   *
   * @param call is the {@link RelOptRuleCall} to this rule.
   */
  @Override public void onMatch(final RelOptRuleCall call) {
    // Gets the two original join operators.
    final Join topJoin = call.rel(0);
    final Join bottomJoin = call.rel(1);

    // Checks whether the join type is valid for l-asscom.
    final JoinRelType topJoinType = topJoin.getJoinType();
    final JoinRelType bottomJoinType = bottomJoin.getJoinType();
    final Pair<JoinRelType, JoinRelType> joinTypePair = Pair.of(topJoinType, bottomJoinType);
    if (!VALID_PAIRS.contains(joinTypePair)) {
      LOGGER.debug("Unable to apply l-asscom on " + joinTypePair);
      return;
    }

    // Checks whether the predicate is referring to the correct relation.
    final int topRight = topJoin.getRight().getRowType().getFieldCount();
    final int bottomLeft = bottomJoin.getLeft().getRowType().getFieldCount();
    final int bottomRight = bottomJoin.getRight().getRowType().getFieldCount();
    final int bottom = bottomJoin.getRowType().getFieldCount();
    if (RelOptCustomUtil.isReferringTo(topJoin.getCondition(),
        topJoin.getRowType().getFieldList().subList(bottomLeft, bottom))) {
      LOGGER.debug("Not a subset of attributes.");
      return;
    }

    // Replaces the variables in the predicates later.
    final RexBuilder rexBuilder = topJoin.getCluster().getRexBuilder();
    final AsscomLeftVariableReplacer replacer = new AsscomLeftVariableReplacer(
        rexBuilder, topRight, bottomLeft, bottomRight, topJoinType, bottomJoinType);

    // The new operators.
    final Join newBottomJoin = topJoin.copy(
        topJoin.getTraitSet(),
        replacer.replaceFromTopToBottom(topJoin.getCondition()),
        bottomJoin.getLeft(),
        topJoin.getRight(),
        topJoinType,
        topJoin.isSemiJoinDone());
    final Join newTopJoin = bottomJoin.copy(
        bottomJoin.getTraitSet(),
        replacer.replaceFromBottomToTop(bottomJoin.getCondition()),
        newBottomJoin,
        bottomJoin.getRight(),
        bottomJoinType,
        bottomJoin.isSemiJoinDone());

    // Builds the transformed relational tree.
    final RelNode projectedNode = call.builder().push(newTopJoin)
        .project(replacer.getProjectFields(newTopJoin)).build();
    call.transformTo(projectedNode);
  }
}

// End AsscomGeneralLeftRule.java
