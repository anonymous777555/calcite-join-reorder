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
import org.apache.calcite.rel.core.Nullify;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Nullify} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableNullify extends Nullify implements EnumerableRel {
  /**
   * Creates a enumerable nullification operator.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traits    the traits of this rel
   * @param child     input relational expression
   * @param predicate the nullification predicate
   * @param attributes the list of nullification attributes
   */
  public EnumerableNullify(RelOptCluster cluster, RelTraitSet traits,
      RelNode child, RexNode predicate, List<? extends RexNode> attributes) {
    super(cluster, traits, child, predicate, attributes);
    assert getConvention() instanceof EnumerableConvention;
  }

  @Override public Nullify copy(RelTraitSet traitSet, RelNode input, RexNode predicate,
      List<RexNode> attributes) {
    return new EnumerableNullify(getCluster(), traitSet, input, predicate, attributes);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    throw new UnsupportedOperationException();
  }
}

// End EnumerableNullify.java
