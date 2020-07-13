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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A utility class to replace the index of attributes for (derived) r-asscom.
 */
public class AsscomVariableReplacer {
  private final RexBuilder rexBuilder;
  private final int topLeft;
  private final int bottomLeft;
  private final int bottomRight;

  AsscomVariableReplacer(final RexBuilder rexBuilder,
      final int topLeft, final int bottomLeft, final int bottomRight) {
    this.rexBuilder = rexBuilder;
    this.topLeft = topLeft;
    this.bottomLeft = bottomLeft;
    this.bottomRight = bottomRight;
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
      int newIndex;
      if (var.getIndex() < bottomLeft) {
        newIndex = var.getIndex() + topLeft + bottomRight;
      } else {
        newIndex = var.getIndex() - bottomLeft + topLeft;
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
      int newIndex;
      if (var.getIndex() < topLeft) {
        newIndex = var.getIndex();
      } else {
        newIndex = var.getIndex() - bottomLeft;
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

    // Adjusts the internal ordering based on assoc-com property.
    final List<RexNode> result = new ArrayList<>();
    result.addAll(inputFields.subList(0, topLeft));
    result.addAll(inputFields.subList(topLeft + bottomRight, inputFields.size()));
    result.addAll(inputFields.subList(topLeft, topLeft + bottomRight));
    return result;
  }
}

// End AsscomVariableReplacer.java
