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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.NullifyStar;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.NullifyStar}
 * not targeted at any particular engine or calling convention.
 */
public class LogicalNullifyStar extends NullifyStar {
  private final ImmutableSet<CorrelationId> variablesSet;

  //~ Constructors -----------------------------------------------------------

  protected LogicalNullifyStar(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexNode predicate,
      List<? extends RexNode> attributesA,
      List<? extends RexNode> attributesB,
      ImmutableSet<CorrelationId> variablesSet) {
    super(cluster, traits, child, predicate, attributesA, attributesB);
    this.variablesSet = Objects.requireNonNull(variablesSet);
  }

  /**
   * Creates a LogicalNullifyStar by parsing serialized output.
   */
  public LogicalNullifyStar(RelInput input) {
    super(input);
    this.variablesSet = ImmutableSet.of();
  }

  /** Creates a LogicalNullifyStar. */
  public static LogicalNullifyStar create(final RelNode input, final RexNode condition,
      final List<? extends RexNode> attributesA,
      final List<? extends RexNode> attributesB,
      final ImmutableSet<CorrelationId> variablesSet) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.filter(mq, input))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.filter(mq, input));
    return new LogicalNullifyStar(cluster, traitSet, input, condition,
        attributesA, attributesB, variablesSet);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public Set<CorrelationId> getVariablesSet() {
    return variablesSet;
  }

  @Override public NullifyStar copy(RelTraitSet traitSet, RelNode input,
      RexNode predicate, List<RexNode> attributesA, List<RexNode> attributesB) {
    return new LogicalNullifyStar(getCluster(), traitSet, input, predicate,
        attributesA, attributesB, variablesSet);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public RelWriter explainTerms(final RelWriter pw) {
    return super.explainTerms(pw).itemIf("variablesSet", variablesSet, !variablesSet.isEmpty());
  }
}

// End LogicalNullifyStar.java
