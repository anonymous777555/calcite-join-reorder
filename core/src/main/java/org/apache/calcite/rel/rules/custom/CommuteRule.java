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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;

import java.util.Set;

/**
 * CommuteRule applies commutativity on valid join types.
 */
public class CommuteRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final CommuteRule INSTANCE = new CommuteRule(
      operand(Join.class, any()), null);

  // All valid join types according to TBA.
  private static final Set<JoinRelType> VALID_TYPES = ImmutableSet.of(
      JoinRelType.INNER, JoinRelType.FULL);

  //~ Constructors -----------------------------------------------------------

  public CommuteRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public CommuteRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   *
   * @param call is the {@link RelOptRuleCall} to this rule.
   */
  @Override public void onMatch(final RelOptRuleCall call) {
    final Join join = call.rel(0);

    // Checks whether the join type is valid for commutativity.
    final JoinRelType joinType = join.getJoinType();
    if (!VALID_TYPES.contains(joinType)) {
      LOGGER.debug("Unable to apply commutativity on " + joinType);
      return;
    }

    // Replaces the variables in the predicate.
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final int leftSize = join.getLeft().getRowType().getFieldCount();
    final CommuteVariableReplacer replacer = new CommuteVariableReplacer(rexBuilder, leftSize);

    // Builds the new join.
    final Join newJoin = join.copy(
        join.getTraitSet(),
        replacer.replace(join.getCondition()),
        join.getRight(),
        join.getLeft(),
        joinType,
        join.isSemiJoinDone());
    call.transformTo(newJoin);
  }
}

// End CommuteRule.java
