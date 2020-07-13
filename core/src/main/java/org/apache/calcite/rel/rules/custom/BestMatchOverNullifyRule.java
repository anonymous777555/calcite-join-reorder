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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.BestMatch;
import org.apache.calcite.rel.core.Nullify;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * BestMatchOverNullifyRule eliminates all inner best-match operators (sandwiched
 * by a nullification operator) as long as there is a best-match operator at the
 * very end. It will check whether the nullification predicate is null-intolerant.
 * The conversion is from `B(Nullify(B(R)))` to `B(Nullify(R))`.
 */
public class BestMatchOverNullifyRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** Instance of the current rule. */
  public static final BestMatchOverNullifyRule INSTANCE = new BestMatchOverNullifyRule(
      operand(BestMatch.class,
          operand(Nullify.class,
              operand(BestMatch.class, any()))), null);

  //~ Constructors -----------------------------------------------------------

  public BestMatchOverNullifyRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public BestMatchOverNullifyRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    final Nullify nullify = call.rel(1);
    final BestMatch innerBestMatch = call.rel(2);

    // Makes sure the nullification predicate is null-intolerant (cannot evaluate to TRUE).
    final RexNode predicate = nullify.getPredicate();
    if (RelOptCustomUtil.isNullTolerant(predicate)) {
      throw new AssertionError("The nullification predicate is not null-intolerant.");
    }

    // Constructs the new expression.
    final RelNode transformedNode = call.builder().push(innerBestMatch.getInput())
        .nullify(predicate, nullify.getAttributes()).bestMatch().build();
    call.transformTo(transformedNode);
  }
}

// End BestMatchOverNullifyRule.java
