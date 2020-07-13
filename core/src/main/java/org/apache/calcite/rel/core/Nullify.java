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
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Nullify is an unary operation that performs nullification to the given attribute
 * list of the input relation based on the given predicate.
 */
public abstract class Nullify extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final RexNode predicate;
  protected final ImmutableList<RexNode> attributes;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a nullification operator.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traits    the traits of this rel
   * @param child     input relational expression
   * @param predicate the nullification predicate
   * @param attributes the list of nullification attributes
   */
  protected Nullify(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexNode predicate,
      List<? extends RexNode> attributes) {
    super(cluster, traits, child);
    this.predicate = predicate;
    this.attributes = ImmutableList.copyOf(attributes);
  }

  /**
   * Creates a Nullify by parsing serialized output.
   */
  protected Nullify(RelInput input) {
    this(input.getCluster(),
        input.getTraitSet(),
        input.getInput(),
        input.getExpression("predicate"),
        input.getExpressionList("attributes"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), predicate, attributes);
  }

  public abstract Nullify copy(RelTraitSet traitSet, RelNode input,
      RexNode predicate, List<RexNode> attributes);

  @Override public List<RexNode> getChildExps() {
    return ImmutableList.of(predicate);
  }

  public RelNode accept(RexShuttle shuttle) {
    RexNode predicate = shuttle.apply(this.predicate);
    if (this.predicate == predicate) {
      return this;
    }
    return copy(traitSet, getInput(), predicate, attributes);
  }

  public RexNode getPredicate() {
    return predicate;
  }

  public ImmutableList<RexNode> getAttributes() {
    return ImmutableList.copyOf(attributes);
  }

  @Override public boolean isValid(final Litmus litmus, final Context context) {
    if (!super.isValid(litmus, context)) {
      return litmus.fail(null);
    }

    // Checks whether each field in nullification attributes is valid.
    final Set<RexNode> fields = getRowType().getFieldList().stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toSet());
    for (RexNode attribute: attributes) {
      if (!fields.contains(attribute)) {
        litmus.fail("{} is an invalid field to nullify", attribute);
      }
    }

    // Succeeds otherwise.
    return litmus.succeed();
  }

  @Override protected RelDataType deriveRowType() {
    return SqlValidatorUtil.deriveNullifyRowType(input.getRowType(), attributes,
        getCluster().getTypeFactory());
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final double outputCount = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(outputCount, outputCount, 0);
  }

  @Override public double estimateRowCount(final RelMetadataQuery mq) {
    return mq.getRowCount(input);
  }

  public RelWriter explainTerms(final RelWriter pw) {
    return super.explainTerms(pw)
        .item("predicate", predicate)
        .item("attributes", attributes);
  }
}

// End Nullify.java
