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
import org.apache.calcite.rel.core.BestMatch;

/** Implementation of {@link org.apache.calcite.rel.core.BestMatch} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableBestMatch extends BestMatch implements EnumerableRel {
  /**
   * Creates a enumerable best-match operator.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits  the traits of this rel
   * @param input   Input relational expression
   */
  public EnumerableBestMatch(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, traits, input);
    assert getConvention() instanceof EnumerableConvention;
  }

  @Override public BestMatch copy(RelTraitSet traitSet, RelNode input) {
    return new EnumerableBestMatch(getCluster(), traitSet, input);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    throw new UnsupportedOperationException();
  }
}

// End EnumerableBestMatch.java
