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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.NullifyStar;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.NullifyStar} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableNullifyStar extends NullifyStar implements EnumerableRel {
  /**
   * Creates a enumerable nullification operator.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traits    the traits of this rel
   * @param child     input relational expression
   * @param predicate the nullification predicate
   * @param attributesA the 1st list of nullification attributes
   * @param attributesB the 2nd list of nullification attributes
   */
  public EnumerableNullifyStar(RelOptCluster cluster, RelTraitSet traits, RelNode child,
      RexNode predicate, List<? extends RexNode> attributesA, List<? extends RexNode> attributesB) {
    super(cluster, traits, child, predicate, attributesA, attributesB);
    assert getConvention() instanceof EnumerableConvention;
  }

  @Override public NullifyStar copy(RelTraitSet traitSet, RelNode input, RexNode predicate,
      List<RexNode> attributesA, List<RexNode> attributesB) {
    return new EnumerableNullifyStar(getCluster(), traitSet, input, predicate,
        attributesA, attributesB);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    throw new UnsupportedOperationException();
  }
}

// End EnumerableNullifyStar.java
