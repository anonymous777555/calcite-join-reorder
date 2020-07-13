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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A utility class to replace the index of attributes for l-asscom.
 */
public class AsscomLeftVariableReplacer {
  private final RexBuilder rexBuilder;

  // The sizes of each child.
  private final int topRight;
  private final int bottomLeft;
  private final int bottomRight;

  // The join types.
  private final JoinRelType topJoinType;
  private final JoinRelType bottomJoinType;

  AsscomLeftVariableReplacer(final RexBuilder rexBuilder,
      final int topRight, final int bottomLeft, final int bottomRight,
      final JoinRelType topJoinType, final JoinRelType bottomJoinType) {
    this.rexBuilder = rexBuilder;

    this.topRight = topRight;
    this.bottomLeft = bottomLeft;
    this.bottomRight = bottomRight;

    this.topJoinType = topJoinType;
    this.bottomJoinType = bottomJoinType;
  }

  RexNode replaceFromBottomToTop(final RexNode rex) {
    if (rex instanceof RexCall) {
      final RexCall call = (RexCall) rex;

      // Converts each operand in the predicate.
      ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
      call.operands.forEach(operand -> builder.add(replaceFromBottomToTop(operand)));

      // Re-builds the predicate.
      return call.clone(call.getType(), builder.build());
    } else if (rex instanceof RexInputRef) {
      final RexInputRef var = (RexInputRef) rex;

      // Computes its index after transformation.
      int newIndex = var.getIndex();
      if (newIndex >= bottomLeft && topJoinType.projectsRight()) {
        newIndex = var.getIndex() + topRight;
      }

      // Re-builds the attribute.
      return rexBuilder.makeInputRef(var.getType(), newIndex);
    } else {
      return rex;
    }
  }

  RexNode replaceFromTopToBottom(final RexNode rex) {
    if (rex instanceof RexCall) {
      final RexCall call = (RexCall) rex;

      // Converts each operand in the predicate.
      ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
      call.operands.forEach(operand -> builder.add(replaceFromTopToBottom(operand)));

      // Re-builds the predicate.
      return call.clone(call.getType(), builder.build());
    } else if (rex instanceof RexInputRef) {
      final RexInputRef var = (RexInputRef) rex;

      // Computes its index after transformation.
      int newIndex = var.getIndex();
      if (newIndex >= bottomLeft && bottomJoinType.projectsRight()) {
        newIndex = var.getIndex() - bottomRight;
      }

      // Re-builds the attribute.
      return rexBuilder.makeInputRef(var.getType(), newIndex);
    } else {
      return rex;
    }
  }

  List<RexNode> getProjectFields(final RelNode transformedNode) {
    final List<RexNode> inputFields = transformedNode.getRowType().getFieldList()
        .stream().map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    // No need to adjust when either join does not project its right side.
    if (!topJoinType.projectsRight() || !bottomJoinType.projectsRight()) {
      return inputFields;
    }

    // Adjusts the internal ordering based on assoc-com property.
    final List<RexNode> result = new ArrayList<>();
    result.addAll(inputFields.subList(0, bottomLeft));
    result.addAll(inputFields.subList(bottomLeft + topRight, inputFields.size()));
    result.addAll(inputFields.subList(bottomLeft, bottomLeft + topRight));
    return result;
  }
}

// End AsscomLeftVariableReplacer.java
