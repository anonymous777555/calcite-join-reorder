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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

/**
 * The name-resolution context for expression inside a JOIN clause. The objects
 * visible are the joined table expressions, and those inherited from the parent
 * scope.
 *
 * <p>Consider "SELECT * FROM (A JOIN B ON {exp1}) JOIN C ON {exp2}". {exp1} is
 * resolved in the join scope for "A JOIN B", which contains A and B but not
 * C.</p>
 */
public class JoinScope extends ListScope {
  public static boolean enableJoinScopeFix = false;

  //~ Instance fields --------------------------------------------------------

  private final SqlValidatorScope usingScope;
  private final SqlJoin join;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>JoinScope</code>.
   *
   * @param parent     Parent scope
   * @param usingScope Scope for resolving USING clause
   * @param join       Call to JOIN operator
   */
  JoinScope(
      SqlValidatorScope parent,
      SqlValidatorScope usingScope,
      SqlJoin join) {
    super(parent);
    this.usingScope = usingScope;
    this.join = join;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    return join;
  }

  public void addChild(SqlValidatorNamespace ns, String alias, boolean nullable) {
    super.addChild(ns, alias, nullable);
    if (usingScope != null && usingScope != parent
        && (!enableJoinScopeFix || join.getJoinType().projectsRight() || !isFromRight(ns))) {
      // We're looking at a join within a join. Recursively add this
      // child to its parent scope too. Example:
      //
      //   select *
      //   from (a join b on expr1)
      //   join c on expr2
      //   where expr3
      //
      // 'a' is a child namespace of 'a join b' and also of
      // 'a join b join c'.
      usingScope.addChild(ns, alias, nullable);
    }
  }

  public SqlWindow lookupWindow(String name) {
    // Lookup window in enclosing select.
    if (usingScope != null) {
      return usingScope.lookupWindow(name);
    } else {
      return null;
    }
  }

  /**
   * Returns the scope which is used for resolving USING clause.
   */
  public SqlValidatorScope getUsingScope() {
    return usingScope;
  }

  @Override public boolean isWithin(SqlValidatorScope scope2) {
    if (this == scope2) {
      return true;
    }
    // go from the JOIN to the enclosing SELECT
    return usingScope.isWithin(scope2);
  }

  /**
   * Checks whether a given namespace is from the right side of the join.
   *
   * @param ns is the given namespace.
   * @return true if from the right side.
   */
  private boolean isFromRight(SqlValidatorNamespace ns) {
    try {
      SqlShuttle shuttle = new SqlContainer(ns.getNode());
      join.getRight().accept(shuttle);
      return false;
    } catch (ControlFlowException e) {
      return true;
    }
  }

  /**
   * A subclass of {@link SqlShuttle} that checks whether the given node
   * is contained.
   */
  private static class SqlContainer extends SqlShuttle {
    private final SqlNode target;

    SqlContainer(SqlNode target) {
      this.target = target;
    }

    @Override public SqlNode visit(final SqlIdentifier id) {
      if (target.equalsDeep(id, Litmus.IGNORE)) {
        throw Util.FoundOne.NULL;
      }
      return id;
    }

    @Override public SqlNode visit(final SqlCall call) {
      if (target.equalsDeep(call, Litmus.IGNORE)) {
        throw Util.FoundOne.NULL;
      }
      return super.visit(call);
    }
  }
}

// End JoinScope.java
