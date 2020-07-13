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

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

/**
 * A utility class to replace the index of attributes for commutativity.
 */
public class CommuteVariableReplacer {
  private final RexBuilder rexBuilder;
  private final int left;

  CommuteVariableReplacer(final RexBuilder rexBuilder, final int left) {
    this.rexBuilder = rexBuilder;
    this.left = left;
  }

  RexNode replace(final RexNode rex) {
    if (rex instanceof RexCall) {
      final RexCall call = (RexCall) rex;

      // Converts each operand in the predicate.
      ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
      call.operands.forEach(operand -> builder.add(replace(operand)));

      // Re-builds the predicate.
      return call.clone(call.getType(), builder.build());
    } else if (rex instanceof RexInputRef) {
      final RexInputRef var = (RexInputRef) rex;

      // Re-builds the attribute.
      int newIndex = var.getIndex();
      if (newIndex < left) {
        newIndex += left;
      } else {
        newIndex -= left;
      }
      return rexBuilder.makeInputRef(var.getType(), newIndex);
    } else {
      return rex;
    }
  }
}

// End CommuteVariableReplacer.java
