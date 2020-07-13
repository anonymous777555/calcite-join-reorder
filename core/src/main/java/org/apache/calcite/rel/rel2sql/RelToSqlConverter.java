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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCustomUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.BestMatch;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Nullify;
import org.apache.calcite.rel.core.NullifyStar;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalNullify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility to convert relational expressions to SQL abstract syntax tree.
 */
public class RelToSqlConverter extends SqlImplementor implements ReflectiveVisitor {
  //~ Static fields/initializers ---------------------------------------------

  // Similar to {@link SqlStdOperatorTable#ROW}, but does not print "ROW".
  private static final SqlRowOperator ANONYMOUS_ROW = new SqlRowOperator(" ");

  // A literal "1" for convenience.
  private static final SqlNode ONE = SqlLiteral.createExactNumeric("1", POS);

  // A literal "1 PRECEDING" for convenience when creating WINDOW clause.
  private static final SqlNode ONE_PRECEDING = SqlWindow.createPreceding(ONE, POS);

  // A literal "TRUE" for convenience.
  private static final SqlLiteral TRUE = SqlLiteral.createBoolean(true, POS);

  //~ Instance fields --------------------------------------------------------

  /**
   * A method dispatcher used to send requests to the specific methods for each operator.
   */
  private final ReflectUtil.MethodDispatcher<Result> dispatcher;

  /**
   * A stack to be used during traversal of an operator tree.
   */
  private final Deque<Frame> stack = new ArrayDeque<>();

  /**
   * A flag to control whether it is allowed to bypass {@link RelSubset} when
   * converting to SQL node.
   */
  private boolean allowSubsetConvert = false;

  //~ Methods ----------------------------------------------------------------

  /** Creates a RelToSqlConverter. */
  public RelToSqlConverter(SqlDialect dialect) {
    super(dialect);
    dispatcher = ReflectUtil.createMethodDispatcher(Result.class, this, "visit",
        RelNode.class);
  }

  public RelToSqlConverter(SqlDialect dialect, boolean allowSubsetConvert) {
    this(dialect);
    this.allowSubsetConvert = allowSubsetConvert;
  }

  /** Dispatches a call to the {@code visit(Xxx e)} method where {@code Xxx}
   * most closely matches the runtime type of the argument. */
  protected Result dispatch(RelNode e) {
    return dispatcher.invoke(e);
  }

  public Result visitChild(int i, RelNode e) {
    try {
      stack.push(new Frame(i, e));
      return dispatch(e);
    } finally {
      stack.pop();
    }
  }

  /** @see #dispatch */
  public Result visit(RelNode e) {
    throw new AssertionError("Need to implement " + e.getClass().getName());
  }

  /** @see #dispatch */
  public Result visit(RelSubset e) {
    if (!allowSubsetConvert) {
      throw new AssertionError("Cannot convert " + e.getClass().getName());
    }

    RelNode actual = Util.first(e.getBest(), e.getOriginal());
    Result x = visitChild(0, actual);
    parseCorrelTable(e, x);
    return x;
  }

  /** @see #dispatch */
  public Result visit(Join e) {
    final Result leftResult = visitChild(0, e.getLeft()).resetAlias();
    final Result rightResult = visitChild(1, e.getRight()).resetAlias();
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext = rightResult.qualifiedContext();
    SqlNode sqlCondition = null;
    SqlLiteral condType = JoinConditionType.ON.symbol(POS);
    JoinType joinType = joinType(e.getJoinType());
    if (isCrossJoin(e)) {
      joinType = dialect.emulateJoinTypeForCrossJoin();
      condType = JoinConditionType.NONE.symbol(POS);
    } else {
      sqlCondition = convertConditionToSqlNode(e.getCondition(),
          leftContext,
          rightContext,
          e.getLeft().getRowType().getFieldCount(),
          dialect,
          e.getJoinType());
    }
    SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType.symbol(POS),
            rightResult.asFrom(),
            condType,
            sqlCondition);
    return result(join, leftResult, rightResult);
  }

  private boolean isCrossJoin(final Join e) {
    return e.getJoinType() == JoinRelType.INNER && e.getCondition().isAlwaysTrue();
  }

  /** @see #dispatch */
  public Result visit(Correlate e) {
    final Result leftResult =
        visitChild(0, e.getLeft())
            .resetAlias(e.getCorrelVariable(), e.getRowType());
    parseCorrelTable(e, leftResult);
    final Result rightResult = visitChild(1, e.getRight());
    final SqlNode rightLateral =
        SqlStdOperatorTable.LATERAL.createCall(POS, rightResult.node);
    final SqlNode rightLateralAs =
        SqlStdOperatorTable.AS.createCall(POS, rightLateral,
            new SqlIdentifier(rightResult.neededAlias, POS));

    final SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            JoinType.COMMA.symbol(POS),
            rightLateralAs,
            JoinConditionType.NONE.symbol(POS),
            null);
    return result(join, leftResult, rightResult);
  }

  /** @see #dispatch */
  public Result visit(Filter e) {
    final RelNode input = e.getInput();
    Result x = visitChild(0, input);
    parseCorrelTable(e, x);
    if (input instanceof Aggregate) {
      final Builder builder;
      if (((Aggregate) input).getInput() instanceof Project) {
        builder = x.builder(e);
        builder.clauses.add(Clause.HAVING);
      } else {
        builder = x.builder(e, Clause.HAVING);
      }
      builder.setHaving(builder.context.toSql(null, e.getCondition()));
      return builder.result();
    } else {
      final Builder builder = x.builder(e, Clause.WHERE);
      builder.setWhere(builder.context.toSql(null, e.getCondition()));
      return builder.result();
    }
  }

  /** @see #dispatch */
  public Result visit(Project e) {
    e.getVariablesSet();
    Result x = visitChild(0, e.getInput());
    parseCorrelTable(e, x);
    if (isStar(e.getChildExps(), e.getInput().getRowType(), e.getRowType())) {
      return x;
    }
    final Builder builder =
        x.builder(e, Clause.SELECT);
    final List<SqlNode> selectList = new ArrayList<>();
    for (RexNode ref : e.getChildExps()) {
      SqlNode sqlExpr = builder.context.toSql(null, ref);
      if (SqlUtil.isNullLiteral(sqlExpr, false)) {
        sqlExpr = castNullType(sqlExpr, e.getRowType().getFieldList().get(selectList.size()));
      }
      addSelect(selectList, sqlExpr, e.getRowType());
    }

    builder.setSelect(new SqlNodeList(selectList, POS));
    return builder.result();
  }

  /** @see #dispatch */
  public Result visit(BestMatch e) {
    Result result = visitChild(0, e.getInput());
    Builder builder = result.builder(e, Clause.SELECT, Clause.WHERE);
    parseCorrelTable(e, result);

    // Finds out all primary keys and favorable orderings.
    final List<RelDataTypeField> fields = e.getRowType().getFieldList();
    final Map<String, RelDataTypeField> primaryKeys = RelOptCustomUtil.findPrimaryKeys(e);
    final List<List<String>> orderings = RelOptCustomUtil.generateFavorableOrdering(e);

    // Iterates through each favorable ordering.
    for (final List<String> ordering: orderings) {
      // Includes both fields from current tuple, previous tuple and ROW_NUMBER.
      final List<SqlNode> selectList = constructBestMatchNormalSelectList(builder, fields);
      selectList.addAll(constructBestMatchExtraSelectList(builder, primaryKeys, ordering));
      builder.setSelect(new SqlNodeList(selectList, POS));

      // Builds the intermediate result.
      result = builder.result();
      builder = result.builder(e, Clause.SELECT, Clause.WHERE);

      // Builds the WHERE clause to remove duplicate tuples & dominated tuples.
      final SqlNode whereOperand = constructBestMatchWhere(builder, primaryKeys.values());
      builder.setWhere(whereOperand);
    }

    // Removes extra fields from SELECT clause.
    final List<SqlNode> finalSelectList = constructBestMatchNormalSelectList(builder, fields);
    builder.setSelect(new SqlNodeList(finalSelectList, POS));

    // Builds the final result.
    return builder.result();
  }

  /**
   * Constructs a list of {@link SqlNode} which represents all fields from the input of
   * this {@link BestMatch} operator.
   *
   * @param builder is the SQL builder.
   * @param fields are all input fields.
   * @return a list of {@link SqlNode} to be used in the SELECT clause.
   */
  private List<SqlNode> constructBestMatchNormalSelectList(final Builder builder,
      final List<RelDataTypeField> fields) {
    final List<SqlNode> selectList = new ArrayList<>();
    for (final RelDataTypeField field: fields) {
      final RexNode node = new RexInputRef(field.getIndex(), field.getType());
      final SqlNode sqlNode = builder.context.toSql(null, node);
      selectList.add(sqlNode);
    }
    return selectList;
  }

  /**
   * Constructs a list of {@link SqlNode} which represents all fields by referring the
   * previous tuple or ROW_NUMBER.
   *
   * @param builder is the SQL builder.
   * @param primaryKeys is a mapping from table names to their primary keys.
   * @param favorableOrdering is the favorable ordering to be used.
   * @return a list of {@link SqlNode} to be used in the SELECT clause.
   */
  private List<SqlNode> constructBestMatchExtraSelectList(final Builder builder,
      final Map<String, RelDataTypeField> primaryKeys, final List<String> favorableOrdering) {
    final List<SqlNode> selectList = new ArrayList<>();

    // Creates WINDOW functions to be used later.
    final SqlNode previousWindow = constructBestMatchWindow(builder, true,
        primaryKeys, favorableOrdering);
    final SqlNode currentWindow = constructBestMatchWindow(builder, false,
        primaryKeys, favorableOrdering);

    // Accesses the fields from previous tuple.
    for (final RelDataTypeField field: primaryKeys.values()) {
      final RexNode node = new RexInputRef(field.getIndex(), field.getType());
      final SqlNode sqlNode = builder.context.toSql(null, node);

      // Access the value from previous tuple using OVER clause.
      final SqlNode aggregate = SqlStdOperatorTable.MAX.createCall(POS, sqlNode);
      final SqlNode over = SqlStdOperatorTable.OVER.createCall(POS, aggregate, previousWindow);

      // Creates an alias for the field from the previous tuple.
      final String previousFieldAlias = field.getName() + "_p";
      final SqlNode alias = as(over, previousFieldAlias);
      selectList.add(alias);
    }

    // Checks the ROW_NUMBER of current tuple.
    final SqlNode rowNumber = SqlStdOperatorTable.ROW_NUMBER.createCall(POS);
    final SqlNode over = SqlStdOperatorTable.OVER.createCall(POS, rowNumber, currentWindow);
    final SqlNode alias = as(over, "row_num");
    selectList.add(alias);

    return selectList;
  }

  /**
   * Constructs the WINDOW function to be used in the OVER clause.
   *
   * @param builder is the SQL builder.
   * @param previous indicates whether this WINDOW function should refer to the previous tuple.
   * @param primaryKeys is a mapping from table names to their primary keys.
   * @param favorableOrdering is the favorable ordering to be used.
   * @return a {@link SqlNode} representing the WINDOW function.
   */
  private SqlNode constructBestMatchWindow(final Builder builder, final boolean previous,
      final Map<String, RelDataTypeField> primaryKeys, final List<String> favorableOrdering) {
    // Constructs the ORDER_BY clause in WINDOW function.
    final List<SqlNode> orderByFields = new ArrayList<>();
    for (final String tableName: favorableOrdering) {
      final RelDataTypeField primaryKey = primaryKeys.get(tableName);
      final RexNode node = new RexInputRef(primaryKey.getIndex(), primaryKey.getType());
      final SqlNode sqlNode = builder.context.toSql(null, node);
      orderByFields.add(sqlNode);
    }
    final SqlNodeList orderByNodeList = new SqlNodeList(orderByFields, POS);

    // Creates the WINDOW function.
    final SqlNode bound = previous ? ONE_PRECEDING : null;
    return SqlWindow.create(null, null, SqlNodeList.EMPTY, orderByNodeList, TRUE,
        bound, bound, null, POS);
  }

  /**
   * Constructs the WHERE clause to be used in the implementation of best-match.
   *
   * @param builder is the SQL builder.
   * @param primaryKeys are all primary keys.
   * @return a {@link SqlNode} representing the WHERE clause.
   */
  private SqlNode constructBestMatchWhere(final Builder builder,
      final Collection<RelDataTypeField> primaryKeys) {
    final List<SqlNode> andOperands = new ArrayList<>();

    // Compares the primary keys of current tuple vs previous tuple.
    for (final RelDataTypeField primaryKey: primaryKeys) {
      final RexNode currentTuple = new RexInputRef(primaryKey.getIndex(), primaryKey.getType());
      final SqlNode currentSql = builder.context.toSql(null, currentTuple);

      final String previousFieldAlias = primaryKey.getName() + "_p";
      final SqlNode previousSql = new SqlIdentifier(ImmutableList.of(previousFieldAlias), POS);

      // Case a: current IS NULL.
      final SqlNode currentNull = SqlStdOperatorTable.IS_NULL.createCall(POS, currentSql);

      // Case b: current = previous AND previous IS NOT NULL.
      final SqlNode prevNotNull = SqlStdOperatorTable.IS_NOT_NULL.createCall(POS, previousSql);
      final SqlNode equal = SqlStdOperatorTable.EQUALS.createCall(POS, currentSql, previousSql);
      final SqlNode and = SqlStdOperatorTable.AND.createCall(POS, equal, prevNotNull);

      // Combines the two cases together using OR.
      final SqlNode operand = SqlStdOperatorTable.OR.createCall(POS, currentNull, and);
      andOperands.add(operand);
    }

    // Connects the check for all fields using AND and negates the result.
    SqlNode checkAllFields = andOperands.get(0);
    for (int i = 1; i < andOperands.size(); i++) {
      checkAllFields = SqlStdOperatorTable.AND.createCall(POS, checkAllFields, andOperands.get(i));
    }
    SqlNode negate = SqlStdOperatorTable.NOT.createCall(POS, checkAllFields);

    // Checks whether row_number is 1.
    final SqlNode rowNumField = new SqlIdentifier(ImmutableList.of("row_num"), POS);
    final SqlNode rowNumCheck = SqlStdOperatorTable.EQUALS.createCall(POS, rowNumField, ONE);

    // Finally connects the two parts using OR.
    return SqlStdOperatorTable.OR.createCall(POS, negate, rowNumCheck);
  }

  /** @see #dispatch */
  public Result visit(Nullify e) {
    Result input = visitChild(0, e.getInput());
    parseCorrelTable(e, input);

    return constructNullify(input, e);
  }

  /**
   * Constructs nullification based on a given input.
   *
   * @param inputQuery is the input SQL query.
   * @param nullify is the nullification operator.
   * @return an output SQL query with nullification.
   */
  private Result constructNullify(final Result inputQuery, final Nullify nullify) {
    return constructNullify(inputQuery, nullify, null);
  }

  /** @see #dispatch */
  public Result visit(NullifyStar e) {
    final RelNode input = e.getInput();
    final Result inputQuery = visitChild(0, input);
    parseCorrelTable(e, inputQuery);

    // Prepares the WITH clause for common table expression (CTE).
    final SqlIdentifier cteName = new SqlIdentifier("nullify_star_input", POS);
    final SqlWithItem withItem = new SqlWithItem(POS, cteName, null, inputQuery.node);
    final SqlNodeList withList = SqlNodeList.of(withItem);

    // Constructs the two nullification operator.
    final RexNode predicate = e.getPredicate();
    final ImmutableSet<CorrelationId> set = ImmutableSet.copyOf(e.getVariablesSet());
    final Nullify nullifyA = LogicalNullify.create(input, predicate, e.getAttributesA(), set);
    final Nullify nullifyB = LogicalNullify.create(input, predicate, e.getAttributesB(), set);
    final Result resultA = constructNullify(inputQuery, nullifyA, cteName);
    final Result resultB = constructNullify(inputQuery, nullifyB, cteName);

    // Unions the two nullification together and wraps using a CTE. Notice: here we would use
    // an UNION_ALL rather than UNION. This is because duplicates will be eliminated later by
    // the best-match operator anyway.
    final List<Clause> clauses = ImmutableList.of(Clause.FROM);
    final SqlNode union = SqlStdOperatorTable.UNION_ALL.createCall(POS, resultA.node, resultB.node);
    final SqlWith with = new SqlWith(POS, withList, union);
    return result(with, clauses, e, null);
  }

  /**
   * Constructs nullification based on a given input.
   *
   * @param inputQuery is the input SQL query.
   * @param nullify is the nullification operator.
   * @param inputName is the {@link SqlIdentifier} to replace the FROM clause.
   * @return an output SQL query with nullification.
   */
  private Result constructNullify(final Result inputQuery, final Nullify nullify,
      final SqlIdentifier inputName) {
    final Builder builder = inputQuery.builder(nullify, Clause.SELECT);
    final List<SqlNode> selectList = new ArrayList<>();
    final Set<RexNode> attributes = ImmutableSet.copyOf(nullify.getAttributes());

    // Builds the select.
    for (RelDataTypeField field: nullify.getRowType().getFieldList()) {
      RexNode node = new RexInputRef(field.getIndex(), field.getType());

      // Uses a CASE expression for nullified attributes.
      SqlNode sqlExpr = builder.context.toSql(null, node);
      if (attributes.contains(node)) {
        SqlNodeList whenList = new SqlNodeList(POS);
        whenList.add(builder.context.toSql(null, nullify.getPredicate()));
        SqlNodeList thenList = new SqlNodeList(POS);
        thenList.add(sqlExpr);

        SqlCase sqlCase = new SqlCase(POS, null, whenList, thenList, SqlLiteral.createNull(POS));
        sqlExpr = as(sqlCase, field.getName());
      }
      selectList.add(sqlExpr);
    }

    // Inserts the select into final SQL node tree.
    final SqlNodeList nodeList = new SqlNodeList(selectList, POS);
    builder.setSelect(nodeList);

    // Updates the FROM clause if necessary.
    if (inputName != null) {
      builder.select.setFrom(inputName);
    }
    return builder.result();
  }

  /**
   * Wrap the {@code sqlNodeNull} in a CAST operator with target type as {@code field}.
   * @param sqlNodeNull NULL literal
   * @param field field description of {@code sqlNodeNull}
   * @return null literal wrapped in CAST call.
   */
  private SqlNode castNullType(SqlNode sqlNodeNull, RelDataTypeField field) {
    return SqlStdOperatorTable.CAST.createCall(POS,
            sqlNodeNull, dialect.getCastSpec(field.getType()));
  }

  /** @see #dispatch */
  public Result visit(Aggregate e) {
    return visitAggregate(e, e.getGroupSet().toList());
  }

  private Result visitAggregate(Aggregate e, List<Integer> groupKeyList) {
    // "select a, b, sum(x) from ( ... ) group by a, b"
    final Result x = visitChild(0, e.getInput());
    final Builder builder;
    if (e.getInput() instanceof Project) {
      builder = x.builder(e);
      builder.clauses.add(Clause.GROUP_BY);
    } else {
      builder = x.builder(e, Clause.GROUP_BY);
    }
    final List<SqlNode> selectList = new ArrayList<>();
    final List<SqlNode> groupByList =
        generateGroupList(builder, selectList, e, groupKeyList);
    return buildAggregate(e, builder, selectList, groupByList);
  }

  /**
   * Gets the {@link org.apache.calcite.rel.rel2sql.SqlImplementor.Builder} for
   * the given {@link Aggregate} node.
   *
   * @param e Aggregate node
   * @param inputResult Result from the input
   * @param inputIsProject Whether the input is a Project
   * @return A SQL builder
   */
  protected Builder getAggregateBuilder(Aggregate e, Result inputResult,
      boolean inputIsProject) {
    if (inputIsProject) {
      final Builder builder = inputResult.builder(e);
      builder.clauses.add(Clause.GROUP_BY);
      return builder;
    } else {
      return inputResult.builder(e, Clause.GROUP_BY);
    }
  }

  /**
   * Builds the group list for an Aggregate node.
   *
   * @param e The Aggregate node
   * @param builder The SQL builder
   * @param groupByList output group list
   * @param selectList output select list
   */
  protected void buildAggGroupList(Aggregate e, Builder builder,
      List<SqlNode> groupByList, List<SqlNode> selectList) {
    for (int group : e.getGroupSet()) {
      final SqlNode field = builder.context.field(group);
      addSelect(selectList, field, e.getRowType());
      groupByList.add(field);
    }
  }

  /**
   * Builds an aggregate query.
   *
   * @param e The Aggregate node
   * @param builder The SQL builder
   * @param selectList The precomputed group list
   * @param groupByList The precomputed select list
   * @return The aggregate query result
   */
  protected Result buildAggregate(Aggregate e, Builder builder,
      List<SqlNode> selectList, List<SqlNode> groupByList) {
    for (AggregateCall aggCall : e.getAggCallList()) {
      SqlNode aggCallSqlNode = builder.context.toSql(aggCall);
      if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
        aggCallSqlNode = dialect.rewriteSingleValueExpr(aggCallSqlNode);
      }
      addSelect(selectList, aggCallSqlNode, e.getRowType());
    }
    builder.setSelect(new SqlNodeList(selectList, POS));
    if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
      // Some databases don't support "GROUP BY ()". We can omit it as long
      // as there is at least one aggregate function.
      builder.setGroupBy(new SqlNodeList(groupByList, POS));
    }
    return builder.result();
  }

  /** Generates the GROUP BY items, for example {@code GROUP BY x, y},
   * {@code GROUP BY CUBE (x, y)} or {@code GROUP BY ROLLUP (x, y)}.
   *
   * <p>Also populates the SELECT clause. If the GROUP BY list is simple, the
   * SELECT will be identical; if the GROUP BY list contains GROUPING SETS,
   * CUBE or ROLLUP, the SELECT clause will contain the distinct leaf
   * expressions. */
  private List<SqlNode> generateGroupList(Builder builder,
      List<SqlNode> selectList, Aggregate aggregate, List<Integer> groupList) {
    final List<Integer> sortedGroupList =
        Ordering.natural().sortedCopy(groupList);
    assert aggregate.getGroupSet().asList().equals(sortedGroupList)
        : "groupList " + groupList + " must be equal to groupSet "
        + aggregate.getGroupSet() + ", just possibly a different order";

    final List<SqlNode> groupKeys = new ArrayList<>();
    for (int key : groupList) {
      final SqlNode field = builder.context.field(key);
      groupKeys.add(field);
    }
    for (int key : sortedGroupList) {
      final SqlNode field = builder.context.field(key);
      addSelect(selectList, field, aggregate.getRowType());
    }
    switch (aggregate.getGroupType()) {
    case SIMPLE:
      return ImmutableList.copyOf(groupKeys);
    case CUBE:
      if (aggregate.getGroupSet().cardinality() > 1) {
        return ImmutableList.of(
            SqlStdOperatorTable.CUBE.createCall(SqlParserPos.ZERO, groupKeys));
      }
      // a singleton CUBE and ROLLUP are the same but we prefer ROLLUP;
      // fall through
    case ROLLUP:
      return ImmutableList.of(
          SqlStdOperatorTable.ROLLUP.createCall(SqlParserPos.ZERO, groupKeys));
    default:
    case OTHER:
      return ImmutableList.of(
          SqlStdOperatorTable.GROUPING_SETS.createCall(SqlParserPos.ZERO,
              aggregate.getGroupSets().stream()
                  .map(groupSet ->
                      groupItem(groupKeys, groupSet, aggregate.getGroupSet()))
                  .collect(Collectors.toList())));
    }
  }

  private SqlNode groupItem(List<SqlNode> groupKeys,
      ImmutableBitSet groupSet, ImmutableBitSet wholeGroupSet) {
    final List<SqlNode> nodes = groupSet.asList().stream()
        .map(key -> groupKeys.get(wholeGroupSet.indexOf(key)))
        .collect(Collectors.toList());
    switch (nodes.size()) {
    case 1:
      return nodes.get(0);
    default:
      return SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, nodes);
    }
  }

  /** @see #dispatch */
  public Result visit(TableScan e) {
    final SqlIdentifier identifier;
    final JdbcTable jdbcTable = e.getTable().unwrap(JdbcTable.class);
    if (jdbcTable != null) {
      // Use the foreign catalog, schema and table names, if they exist,
      // rather than the qualified name of the shadow table in Calcite.
      identifier = jdbcTable.tableName();
    } else {
      final List<String> qualifiedName = e.getTable().getQualifiedName();
      identifier = new SqlIdentifier(qualifiedName, SqlParserPos.ZERO);
    }
    return result(identifier, ImmutableList.of(Clause.FROM), e, null);
  }

  /** @see #dispatch */
  public Result visit(Union e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.UNION_ALL
        : SqlStdOperatorTable.UNION, e);
  }

  /** @see #dispatch */
  public Result visit(Intersect e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.INTERSECT_ALL
        : SqlStdOperatorTable.INTERSECT, e);
  }

  /** @see #dispatch */
  public Result visit(Minus e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.EXCEPT_ALL
        : SqlStdOperatorTable.EXCEPT, e);
  }

  /** @see #dispatch */
  public Result visit(Calc e) {
    Result x = visitChild(0, e.getInput());
    parseCorrelTable(e, x);
    final RexProgram program = e.getProgram();
    Builder builder =
        program.getCondition() != null
            ? x.builder(e, Clause.WHERE)
            : x.builder(e);
    if (!isStar(program)) {
      final List<SqlNode> selectList = new ArrayList<>();
      for (RexLocalRef ref : program.getProjectList()) {
        SqlNode sqlExpr = builder.context.toSql(program, ref);
        addSelect(selectList, sqlExpr, e.getRowType());
      }
      builder.setSelect(new SqlNodeList(selectList, POS));
    }

    if (program.getCondition() != null) {
      builder.setWhere(
          builder.context.toSql(program, program.getCondition()));
    }
    return builder.result();
  }

  /** @see #dispatch */
  public Result visit(Values e) {
    final List<Clause> clauses = ImmutableList.of(Clause.SELECT);
    final Map<String, RelDataType> pairs = ImmutableMap.of();
    final Context context = aliasContext(pairs, false);
    SqlNode query;
    final boolean rename = stack.size() <= 1
        || !(Iterables.get(stack, 1).r instanceof TableModify);
    final List<String> fieldNames = e.getRowType().getFieldNames();
    if (!dialect.supportsAliasedValues() && rename) {
      // Oracle does not support "AS t (c1, c2)". So instead of
      //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
      // we generate
      //   SELECT v0 AS c0, v1 AS c1 FROM DUAL
      //   UNION ALL
      //   SELECT v2 AS c0, v3 AS c1 FROM DUAL
      List<SqlSelect> list = new ArrayList<>();
      for (List<RexLiteral> tuple : e.getTuples()) {
        final List<SqlNode> values2 = new ArrayList<>();
        final SqlNodeList exprList = exprList(context, tuple);
        for (Pair<SqlNode, String> value : Pair.zip(exprList, fieldNames)) {
          values2.add(as(value.left, value.right));
        }
        list.add(
            new SqlSelect(POS, null,
                new SqlNodeList(values2, POS),
                getDual(), null, null,
                null, null, null, null, null));
      }
      if (list.isEmpty()) {
        // In this case we need to construct the following query:
        // SELECT NULL as C0, NULL as C1, NULL as C2 ... FROM DUAL WHERE FALSE
        // This would return an empty result set with the same number of columns as the field names.
        final List<SqlNode> nullColumnNames = new ArrayList<>();
        for (String fieldName : fieldNames) {
          SqlCall nullColumnName = as(SqlLiteral.createNull(POS), fieldName);
          nullColumnNames.add(nullColumnName);
        }
        final SqlIdentifier dual = getDual();
        if (dual == null) {
          query = new SqlSelect(POS, null,
              new SqlNodeList(nullColumnNames, POS), null, null, null, null,
              null, null, null, null);

          // Wrap "SELECT 1 AS x"
          // as "SELECT * FROM (SELECT 1 AS x) AS t WHERE false"
          query = new SqlSelect(POS, null,
              new SqlNodeList(ImmutableList.of(SqlIdentifier.star(POS)), POS),
              as(query, "t"), createAlwaysFalseCondition(), null, null,
              null, null, null, null);
        } else {
          query = new SqlSelect(POS, null,
              new SqlNodeList(nullColumnNames, POS),
              dual, createAlwaysFalseCondition(), null,
              null, null, null, null, null);
        }
      } else if (list.size() == 1) {
        query = list.get(0);
      } else {
        query = SqlStdOperatorTable.UNION_ALL.createCall(
            new SqlNodeList(list, POS));
      }
    } else {
      // Generate ANSI syntax
      //   (VALUES (v0, v1), (v2, v3))
      // or, if rename is required
      //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
      final SqlNodeList selects = new SqlNodeList(POS);
      final boolean isEmpty = Values.isEmpty(e);
      if (isEmpty) {
        // In case of empty values, we need to build:
        // select * from VALUES(NULL, NULL ...) as T (C1, C2 ...)
        // where 1=0.
        List<SqlNode> nulls = IntStream.range(0, fieldNames.size())
            .mapToObj(i ->
                SqlLiteral.createNull(POS)).collect(Collectors.toList());
        selects.add(ANONYMOUS_ROW.createCall(new SqlNodeList(nulls, POS)));
      } else {
        for (List<RexLiteral> tuple : e.getTuples()) {
          selects.add(ANONYMOUS_ROW.createCall(exprList(context, tuple)));
        }
      }
      query = SqlStdOperatorTable.VALUES.createCall(selects);
      if (rename) {
        query = as(query, "t", fieldNames.toArray(new String[0]));
      }
      if (isEmpty) {
        if (!rename) {
          query = as(query, "t");
        }
        query = new SqlSelect(POS, null,
                null, query,
                createAlwaysFalseCondition(),
                null, null, null,
                null, null, null);
      }
    }
    return result(query, clauses, e, null);
  }

  private SqlIdentifier getDual() {
    final List<String> names = dialect.getSingleRowTableName();
    if (names == null) {
      return null;
    }
    return new SqlIdentifier(names, POS);
  }

  private SqlNode createAlwaysFalseCondition() {
    // Building the select query in the form:
    // select * from VALUES(NULL,NULL ...) where 1=0
    // Use condition 1=0 since "where false" does not seem to be supported
    // on some DB vendors.
    return SqlStdOperatorTable.EQUALS.createCall(POS,
            ImmutableList.of(SqlLiteral.createExactNumeric("1", POS),
                    SqlLiteral.createExactNumeric("0", POS)));
  }

  /** @see #dispatch */
  public Result visit(Sort e) {
    if (e.getInput() instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) e.getInput();
      if (hasTrickyRollup(e, aggregate)) {
        // MySQL 5 does not support standard "GROUP BY ROLLUP(x, y)", only
        // the non-standard "GROUP BY x, y WITH ROLLUP".
        // It does not allow "WITH ROLLUP" in combination with "ORDER BY",
        // but "GROUP BY x, y WITH ROLLUP" implicitly sorts by x, y,
        // so skip the ORDER BY.
        final Set<Integer> groupList = new LinkedHashSet<>();
        for (RelFieldCollation fc : e.collation.getFieldCollations()) {
          groupList.add(aggregate.getGroupSet().nth(fc.getFieldIndex()));
        }
        groupList.addAll(Aggregate.Group.getRollup(aggregate.getGroupSets()));
        return offsetFetch(e,
            visitAggregate(aggregate, ImmutableList.copyOf(groupList)));
      }
    }
    if (e.getInput() instanceof Project) {
      // Deal with the case Sort(Project(Aggregate ...))
      // by converting it to Project(Sort(Aggregate ...)).
      final Project project = (Project) e.getInput();
      final Permutation permutation = project.getPermutation();
      if (permutation != null
          && project.getInput() instanceof Aggregate) {
        final Aggregate aggregate = (Aggregate) project.getInput();
        if (hasTrickyRollup(e, aggregate)) {
          final RelCollation collation =
              RelCollations.permute(e.collation, permutation);
          final Sort sort2 =
              LogicalSort.create(aggregate, collation, e.offset, e.fetch);
          final Project project2 =
              LogicalProject.create(sort2, project.getProjects(),
                  project.getRowType());
          return visit(project2);
        }
      }
    }
    Result x = visitChild(0, e.getInput());
    Builder builder = x.builder(e, Clause.ORDER_BY);
    List<SqlNode> orderByList = Expressions.list();
    for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
      builder.addOrderItem(orderByList, field);
    }
    if (!orderByList.isEmpty()) {
      builder.setOrderBy(new SqlNodeList(orderByList, POS));
      x = builder.result();
    }
    x = offsetFetch(e, x);
    return x;
  }

  Result offsetFetch(Sort e, Result x) {
    if (e.fetch != null) {
      final Builder builder = x.builder(e, Clause.FETCH);
      builder.setFetch(builder.context.toSql(null, e.fetch));
      x = builder.result();
    }
    if (e.offset != null) {
      final Builder builder = x.builder(e, Clause.OFFSET);
      builder.setOffset(builder.context.toSql(null, e.offset));
      x = builder.result();
    }
    return x;
  }

  public boolean hasTrickyRollup(Sort e, Aggregate aggregate) {
    return !dialect.supportsAggregateFunction(SqlKind.ROLLUP)
        && dialect.supportsGroupByWithRollup()
        && (aggregate.getGroupType() == Aggregate.Group.ROLLUP
            || aggregate.getGroupType() == Aggregate.Group.CUBE
                && aggregate.getGroupSet().cardinality() == 1)
        && e.collation.getFieldCollations().stream().allMatch(fc ->
            fc.getFieldIndex() < aggregate.getGroupSet().cardinality());
  }

  /** @see #dispatch */
  public Result visit(TableModify modify) {
    final Map<String, RelDataType> pairs = ImmutableMap.of();
    final Context context = aliasContext(pairs, false);

    // Target Table Name
    final SqlIdentifier sqlTargetTable =
        new SqlIdentifier(modify.getTable().getQualifiedName(), POS);

    switch (modify.getOperation()) {
    case INSERT: {
      // Convert the input to a SELECT query or keep as VALUES. Not all
      // dialects support naked VALUES, but all support VALUES inside INSERT.
      final SqlNode sqlSource =
          visitChild(0, modify.getInput()).asQueryOrValues();

      final SqlInsert sqlInsert =
          new SqlInsert(POS, SqlNodeList.EMPTY, sqlTargetTable, sqlSource,
              identifierList(modify.getInput().getRowType().getFieldNames()));

      return result(sqlInsert, ImmutableList.of(), modify, null);
    }
    case UPDATE: {
      final Result input = visitChild(0, modify.getInput());

      final SqlUpdate sqlUpdate =
          new SqlUpdate(POS, sqlTargetTable,
              identifierList(modify.getUpdateColumnList()),
              exprList(context, modify.getSourceExpressionList()),
              ((SqlSelect) input.node).getWhere(), input.asSelect(),
              null);

      return result(sqlUpdate, input.clauses, modify, null);
    }
    case DELETE: {
      final Result input = visitChild(0, modify.getInput());

      final SqlDelete sqlDelete =
          new SqlDelete(POS, sqlTargetTable,
              input.asSelect().getWhere(), input.asSelect(), null);

      return result(sqlDelete, input.clauses, modify, null);
    }
    case MERGE:
    default:
      throw new AssertionError("not implemented: " + modify);
    }
  }

  /** Converts a list of {@link RexNode} expressions to {@link SqlNode}
   * expressions. */
  private SqlNodeList exprList(final Context context,
      List<? extends RexNode> exprs) {
    return new SqlNodeList(
        Lists.transform(exprs, e -> context.toSql(null, e)), POS);
  }

  /** Converts a list of names expressions to a list of single-part
   * {@link SqlIdentifier}s. */
  private SqlNodeList identifierList(List<String> names) {
    return new SqlNodeList(
        Lists.transform(names, name -> new SqlIdentifier(name, POS)), POS);
  }

  /** @see #dispatch */
  public Result visit(Match e) {
    final RelNode input = e.getInput();
    final Result x = visitChild(0, input);
    final Context context = matchRecognizeContext(x.qualifiedContext());

    SqlNode tableRef = x.asQueryOrValues();

    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    final List<SqlNode> partitionSqlList = new ArrayList<>();
    for (int key : e.getPartitionKeys()) {
      final RexInputRef ref = rexBuilder.makeInputRef(input, key);
      SqlNode sqlNode = context.toSql(null, ref);
      partitionSqlList.add(sqlNode);
    }
    final SqlNodeList partitionList = new SqlNodeList(partitionSqlList, POS);

    final List<SqlNode> orderBySqlList = new ArrayList<>();
    if (e.getOrderKeys() != null) {
      for (RelFieldCollation fc : e.getOrderKeys().getFieldCollations()) {
        if (fc.nullDirection != RelFieldCollation.NullDirection.UNSPECIFIED) {
          boolean first = fc.nullDirection == RelFieldCollation.NullDirection.FIRST;
          SqlNode nullDirectionNode =
              dialect.emulateNullDirection(context.field(fc.getFieldIndex()),
                  first, fc.direction.isDescending());
          if (nullDirectionNode != null) {
            orderBySqlList.add(nullDirectionNode);
            fc = new RelFieldCollation(fc.getFieldIndex(), fc.getDirection(),
                RelFieldCollation.NullDirection.UNSPECIFIED);
          }
        }
        orderBySqlList.add(context.toSql(fc));
      }
    }
    final SqlNodeList orderByList = new SqlNodeList(orderBySqlList, SqlParserPos.ZERO);

    final SqlLiteral rowsPerMatch = e.isAllRows()
        ? SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS.symbol(POS)
        : SqlMatchRecognize.RowsPerMatchOption.ONE_ROW.symbol(POS);

    final SqlNode after;
    if (e.getAfter() instanceof RexLiteral) {
      SqlMatchRecognize.AfterOption value = (SqlMatchRecognize.AfterOption)
          ((RexLiteral) e.getAfter()).getValue2();
      after = SqlLiteral.createSymbol(value, POS);
    } else {
      RexCall call = (RexCall) e.getAfter();
      String operand = RexLiteral.stringValue(call.getOperands().get(0));
      after = call.getOperator().createCall(POS, new SqlIdentifier(operand, POS));
    }

    RexNode rexPattern = e.getPattern();
    final SqlNode pattern = context.toSql(null, rexPattern);
    final SqlLiteral strictStart = SqlLiteral.createBoolean(e.isStrictStart(), POS);
    final SqlLiteral strictEnd = SqlLiteral.createBoolean(e.isStrictEnd(), POS);

    RexLiteral rexInterval = (RexLiteral) e.getInterval();
    SqlIntervalLiteral interval = null;
    if (rexInterval != null) {
      interval = (SqlIntervalLiteral) context.toSql(null, rexInterval);
    }

    final SqlNodeList subsetList = new SqlNodeList(POS);
    for (Map.Entry<String, SortedSet<String>> entry : e.getSubsets().entrySet()) {
      SqlNode left = new SqlIdentifier(entry.getKey(), POS);
      List<SqlNode> rhl = new ArrayList<>();
      for (String right : entry.getValue()) {
        rhl.add(new SqlIdentifier(right, POS));
      }
      subsetList.add(
          SqlStdOperatorTable.EQUALS.createCall(POS, left,
              new SqlNodeList(rhl, POS)));
    }

    final SqlNodeList measureList = new SqlNodeList(POS);
    for (Map.Entry<String, RexNode> entry : e.getMeasures().entrySet()) {
      final String alias = entry.getKey();
      final SqlNode sqlNode = context.toSql(null, entry.getValue());
      measureList.add(as(sqlNode, alias));
    }

    final SqlNodeList patternDefList = new SqlNodeList(POS);
    for (Map.Entry<String, RexNode> entry : e.getPatternDefinitions().entrySet()) {
      final String alias = entry.getKey();
      final SqlNode sqlNode = context.toSql(null, entry.getValue());
      patternDefList.add(as(sqlNode, alias));
    }

    final SqlNode matchRecognize = new SqlMatchRecognize(POS, tableRef,
        pattern, strictStart, strictEnd, patternDefList, measureList, after,
        subsetList, rowsPerMatch, partitionList, orderByList, interval);
    return result(matchRecognize, Expressions.list(Clause.FROM), e, null);
  }

  private SqlCall as(SqlNode e, String alias) {
    return SqlStdOperatorTable.AS.createCall(POS, e,
        new SqlIdentifier(alias, POS));
  }

  public Result visit(Uncollect e) {
    final Result x = visitChild(0, e.getInput());
    final SqlNode unnestNode = SqlStdOperatorTable.UNNEST.createCall(POS, x.asStatement());
    final List<SqlNode> operands = createAsFullOperands(e.getRowType(), unnestNode, x.neededAlias);
    final SqlNode asNode = SqlStdOperatorTable.AS.createCall(POS, operands);
    return result(asNode, ImmutableList.of(Clause.FROM), e, null);
  }

  /**
   * Creates operands for a full AS operator. Format SqlNode AS alias(col_1, col_2,... ,col_n).
   *
   * @param rowType Row type of the SqlNode
   * @param leftOperand SqlNode
   * @param alias alias
   */
  public List<SqlNode> createAsFullOperands(RelDataType rowType, SqlNode leftOperand,
      String alias) {
    final List<SqlNode> result = new ArrayList<>();
    result.add(leftOperand);
    result.add(new SqlIdentifier(alias, POS));
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      final String lowerName = rowType.getFieldNames().get(i).toLowerCase(Locale.ROOT);
      SqlIdentifier sqlColumn;
      if (lowerName.startsWith("expr$")) {
        sqlColumn = new SqlIdentifier("col_" + i, POS);
        ordinalMap.put(lowerName, sqlColumn);
      } else {
        sqlColumn = new SqlIdentifier(rowType.getFieldNames().get(i), POS);
      }
      result.add(sqlColumn);
    }
    return result;
  }

  @Override public void addSelect(List<SqlNode> selectList, SqlNode node,
      RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    String alias = SqlValidatorUtil.getAlias(node, -1);
    final String lowerName = name.toLowerCase(Locale.ROOT);
    if (lowerName.startsWith("expr$")) {
      // Put it in ordinalMap
      ordinalMap.put(lowerName, node);
    } else if (alias == null || !alias.equals(name)) {
      node = as(node, name);
    }
    selectList.add(node);
  }

  private void parseCorrelTable(RelNode relNode, Result x) {
    for (CorrelationId id : relNode.getVariablesSet()) {
      correlTableMap.put(id, x.qualifiedContext());
    }
  }

  /** Stack frame. */
  private static class Frame {
    private final int ordinalInParent;
    private final RelNode r;

    Frame(int ordinalInParent, RelNode r) {
      this.ordinalInParent = ordinalInParent;
      this.r = r;
    }
  }
}

// End RelToSqlConverter.java
