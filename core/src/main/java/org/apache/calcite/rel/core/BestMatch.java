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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * BestMatch is a unary operation that is always at the top of a join tree
 * to eliminate spurious tuples.
 */
public abstract class BestMatch extends SingleRel {
  // A tunable constant on the percentage of tuples to be removed.
  private static final double REMOVAL_PERCENTAGE = 0.1;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a best-match operator.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits  the traits of this rel
   * @param input   Input relational expression
   */
  protected BestMatch(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, traits, input);
  }

  /**
   * Creates a best match by its input.
   */
  protected BestMatch(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput());
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public abstract BestMatch copy(RelTraitSet traitSet, RelNode input);

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final double outputCount = mq.getRowCount(input);
    final double bytesPerRow = getRowType().getFieldCount() * 4;
    final double cpu = Util.nLogN(outputCount) * bytesPerRow;
    return planner.getCostFactory().makeCost(outputCount, cpu, 0);
  }

  @Override public double estimateRowCount(final RelMetadataQuery mq) {
    return mq.getRowCount(input) * (1 - REMOVAL_PERCENTAGE);
  }
}

// End BestMatch.java
