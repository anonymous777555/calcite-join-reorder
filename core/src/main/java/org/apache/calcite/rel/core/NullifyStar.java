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
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * NullifyStar is an unary operation that performs nullification to the two given
 * attribute lists of the input relation based on the given predicate.
 */
public abstract class NullifyStar extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final RexNode predicate;
  protected final ImmutableList<RexNode> attributesA;
  protected final ImmutableList<RexNode> attributesB;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a nullification star operator.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traits    the traits of this rel
   * @param child     input relational expression
   * @param predicate the nullification predicate
   * @param attributesA the 1st list of nullification attributes
   * @param attributesB the 2nd list of nullification attributes
   */
  protected NullifyStar(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexNode predicate,
      List<? extends RexNode> attributesA,
      List<? extends RexNode> attributesB) {
    super(cluster, traits, child);
    this.predicate = predicate;
    this.attributesA = ImmutableList.copyOf(attributesA);
    this.attributesB = ImmutableList.copyOf(attributesB);
  }

  /**
   * Creates a NullifyStar by parsing serialized output.
   */
  protected NullifyStar(RelInput input) {
    this(input.getCluster(),
        input.getTraitSet(),
        input.getInput(),
        input.getExpression("predicate"),
        input.getExpressionList("attributesA"),
        input.getExpressionList("attributesB"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), predicate, attributesA, attributesB);
  }

  public abstract NullifyStar copy(RelTraitSet traitSet, RelNode input,
      RexNode predicate, List<RexNode> attributesA, List<RexNode> attributesB);

  @Override public List<RexNode> getChildExps() {
    return ImmutableList.of(predicate);
  }

  public RelNode accept(RexShuttle shuttle) {
    RexNode predicate = shuttle.apply(this.predicate);
    if (this.predicate == predicate) {
      return this;
    }
    return copy(traitSet, getInput(), predicate, attributesA, attributesB);
  }

  public RexNode getPredicate() {
    return predicate;
  }

  public ImmutableList<RexNode> getAttributesA() {
    return attributesA;
  }

  public ImmutableList<RexNode> getAttributesB() {
    return attributesB;
  }

  @Override public boolean isValid(final Litmus litmus, final Context context) {
    if (!super.isValid(litmus, context)) {
      return litmus.fail(null);
    }

    // Checks whether each field in two nullification lists is valid.
    final Set<RexNode> fields = getRowType().getFieldList().stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toSet());
    for (RexNode attribute: attributesA) {
      if (!fields.contains(attribute)) {
        litmus.fail("{} is an invalid field to nullify in 1st list", attribute);
      }
    }
    for (RexNode attribute: attributesB) {
      if (!fields.contains(attribute)) {
        litmus.fail("{} is an invalid field to nullify in 2nd list", attribute);
      }
    }

    // Succeeds otherwise.
    return litmus.succeed();
  }

  @Override protected RelDataType deriveRowType() {
    final List<RexNode> fields = new ArrayList<>(attributesA);
    fields.addAll(attributesB);

    return SqlValidatorUtil.deriveNullifyRowType(input.getRowType(), fields,
        getCluster().getTypeFactory());
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final double inputCount = mq.getRowCount(input);
    final double outputCount = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(outputCount * 2, inputCount * 2, 0);
  }

  @Override public double estimateRowCount(final RelMetadataQuery mq) {
    final double inputCount = mq.getRowCount(input);
    final double filterCount = RelMdUtil.estimateFilteredRows(input, predicate, mq);
    return inputCount * 2 - filterCount;
  }

  public RelWriter explainTerms(final RelWriter pw) {
    return super.explainTerms(pw)
        .item("predicate", predicate)
        .item("attributesA", attributesA)
        .item("attributesB", attributesB);
  }
}

// End NullifyStar.java
