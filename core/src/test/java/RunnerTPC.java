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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptCustomUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCustomRunner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.custom.AsscomGeneralLeftRule;
import org.apache.calcite.rel.rules.custom.AsscomGeneralRightRule;
import org.apache.calcite.rel.rules.custom.AsscomInnerAntiRule;
import org.apache.calcite.rel.rules.custom.AsscomInnerLeftRule;
import org.apache.calcite.rel.rules.custom.AsscomLeftAntiRule;
import org.apache.calcite.rel.rules.custom.AsscomLeftInnerRule;
import org.apache.calcite.rel.rules.custom.AsscomLeftLeftRule;
import org.apache.calcite.rel.rules.custom.AssocFullAntiRule;
import org.apache.calcite.rel.rules.custom.AssocFullInnerRule;
import org.apache.calcite.rel.rules.custom.AssocGeneralRule;
import org.apache.calcite.rel.rules.custom.AssocLeftAntiRule;
import org.apache.calcite.rel.rules.custom.AssocLeftFullRule;
import org.apache.calcite.rel.rules.custom.AssocLeftInnerRule;
import org.apache.calcite.rel.rules.custom.AssocReverseAntiAntiRule;
import org.apache.calcite.rel.rules.custom.AssocReverseAntiFullRule;
import org.apache.calcite.rel.rules.custom.AssocReverseAntiLeftRule;
import org.apache.calcite.rel.rules.custom.AssocReverseFullAntiRule;
import org.apache.calcite.rel.rules.custom.AssocReverseFullLeftRule;
import org.apache.calcite.rel.rules.custom.AssocReverseInnerAntiRule;
import org.apache.calcite.rel.rules.custom.AssocReverseInnerFullRule;
import org.apache.calcite.rel.rules.custom.AssocReverseInnerLeftRule;
import org.apache.calcite.rel.rules.custom.AssocReverseLeftAntiRule;
import org.apache.calcite.rel.rules.custom.BestMatchNullifyPullUpRule;
import org.apache.calcite.rel.rules.custom.BestMatchNullifyStarPullUpRule;
import org.apache.calcite.rel.rules.custom.BestMatchReduceRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import org.apache.commons.dbcp2.BasicDataSource;

import net.jcip.annotations.NotThreadSafe;

import org.junit.Before;
import org.junit.Test;
import org.postgresql.Driver;

/**
 * A runner class for manual testing on TPC-H schema.
 */
@NotThreadSafe
public class RunnerTPC {
  // Default parameters for JDBC connections.
  private static final String DEFAULT_DRIVER_NAME = Driver.class.getName();
  private static final String DEFAULT_DATABASE = "tpc";
  private static final String DEFAULT_USER = "calcite";
  private static final String DEFAULT_PASSWORD = "calcite";

  // The default namespace to be used.
  private static final String DEFAULT_NAME = "db";
  private static final String DEFAULT_NAMESPACE = "public";

  // Defines the default dialect used for output query in this class.
  private static final SqlDialect DEFAULT_DIALECT
      = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
  private static final RelToSqlConverter CONVERTER = new RelToSqlConverter(DEFAULT_DIALECT);

  // All rules that we would like to consider.
  private static final Program ALL_RULES = Programs.ofRules(
      // All enumerable rules.
      EnumerableRules.ENUMERABLE_BEST_MATCH_RULE,
      EnumerableRules.ENUMERABLE_JOIN_RULE,
      EnumerableRules.ENUMERABLE_NULLIFY_RULE,
      EnumerableRules.ENUMERABLE_NULLIFY_STAR_RULE,
      EnumerableRules.ENUMERABLE_PROJECT_RULE,
      EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,

      // Our custom rules.
      AsscomGeneralLeftRule.INSTANCE,
      AsscomGeneralRightRule.INSTANCE,
      AsscomInnerAntiRule.INSTANCE,
      AsscomInnerLeftRule.INSTANCE,
      AsscomLeftAntiRule.INSTANCE,
      AsscomLeftInnerRule.INSTANCE,
      AsscomLeftLeftRule.INSTANCE,
      AssocFullAntiRule.INSTANCE,
      AssocFullInnerRule.INSTANCE,
      AssocGeneralRule.INSTANCE,
      AssocLeftAntiRule.INSTANCE,
      AssocLeftFullRule.INSTANCE,
      AssocLeftInnerRule.INSTANCE,
      AssocReverseAntiAntiRule.INSTANCE,
      AssocReverseAntiFullRule.INSTANCE,
      AssocReverseAntiLeftRule.INSTANCE,
      AssocReverseFullAntiRule.INSTANCE,
      AssocReverseFullLeftRule.INSTANCE,
      AssocReverseInnerAntiRule.INSTANCE,
      AssocReverseInnerFullRule.INSTANCE,
      AssocReverseInnerLeftRule.INSTANCE,
      AssocReverseLeftAntiRule.INSTANCE,

      // Pull up best-match / nullify / nullify-star / projection.
      BestMatchNullifyPullUpRule.LEFT_CHILD,
      BestMatchNullifyPullUpRule.RIGHT_CHILD,
      BestMatchNullifyStarPullUpRule.LEFT_CHILD,
      BestMatchNullifyStarPullUpRule.RIGHT_CHILD,
      BestMatchReduceRule.INSTANCE,
      JoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER,
      JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER,
      JoinProjectTransposeRule.BOTH_PROJECT_INCLUDE_OUTER
  );

  @Before public void setUp() {
    // Disables all assertions.
    getClass().getClassLoader().setDefaultAssertionStatus(false);

    // Encourages relations with none convention.
    VolcanoPlanner.encourageNoneConvention = true;

    // Encourages JDBC conversion.
    RelOptCustomUtil.defaultPreferJDBC = true;
  }

  @Test public void simpleTest() throws Exception {
    final BasicDataSource dataSource = getDataSource();
    final SchemaPlus schema = getSchema(dataSource);

    // 0. A complex query.
    String sqlQuery = ""
        + "SELECT p_name, l_suppkey, o_orderkey, o_orderstatus FROM (\n"
        + "    (SELECT * FROM part WHERE p_retailprice > %d) as part FULL JOIN (\n"
        + "        (SELECT * FROM lineitem WHERE l_quantity > %d) AS lineitem\n"
        + "        INNER JOIN\n"
        + "        (SELECT * FROM orders WHERE o_totalprice > %d) AS orders\n"
        + "        ON l_orderkey = o_orderkey AND l_extendedprice > %f * o_totalprice \n"
        + "    ) as lineitem_orders ON p_partkey = l_partkey\n"
        + ") as part_lineitem WHERE NOT EXISTS (\n"
        + "    SELECT 1 FROM partsupp\n"
        + "    WHERE ps_partkey = p_partkey AND ps_availqty > %d\n"
        + ")\n";
    Program programs = Programs.ofRules();
    Util.discard(sqlQuery);

    // 1. A simple expression without join.
    sqlQuery = "SELECT n_nationkey, n_name, n_comment FROM nation WHERE n_regionkey = 0";
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery, false, true);

    // 2. Rule 1 - full join + inner join.
    sqlQuery = "SELECT s_suppkey, s_name, n_name, r_name FROM supplier "
        + "FULL JOIN nation ON s_nationkey = n_nationkey "
        + "INNER JOIN region ON n_regionkey = r_regionkey";
    programs = Programs.ofRules(AssocFullInnerRule.INSTANCE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery, true, true);

    // 3. Rule 6 - left join + full join.
    sqlQuery = "SELECT * FROM lineitem "
        + "LEFT JOIN orders ON l_orderkey = o_orderkey "
        + "FULL JOIN customer ON o_custkey = c_custkey";
    programs = Programs.ofRules(AssocLeftFullRule.INSTANCE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery, true, true);

    // Closes the connection.
    dataSource.close();
  }

  @Test public void complexTest1() throws Exception {
    final BasicDataSource dataSource = getDataSource();
    final SchemaPlus schema = getSchema(dataSource);
    final FrameworkConfig config = RelCustomRunner.getConfig(ALL_RULES, schema);

    // Builds the relation.
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode relNode = buildComplexQuery1(builder, 1000, 1, 40000, 0.01, 1000);

    // Transforms to SQL node.
    SqlImplementor.enableSelectListFix = true;
    final SqlNode sqlNode = CONVERTER.visitChild(0, relNode).asStatement();
    System.out.println("Original SQL query:");
    System.out.println(sqlNode.toSqlString(DEFAULT_DIALECT) + "\n");
    SqlImplementor.enableSelectListFix = false;

    // Transforms the relation.
    final Planner planner = Frameworks.getPlanner(config);
    final String transformed = RelCustomRunner.transformQuery(planner, sqlNode, true, true);
    System.out.println("Optimized SQL query:");
    System.out.println(transformed + "\n");

    // Closes the resources.
    dataSource.close();
    planner.close();
  }

  @Test public void complexTest2() throws Exception {
    final BasicDataSource dataSource = getDataSource();
    final SchemaPlus schema = getSchema(dataSource);
    final FrameworkConfig config = RelCustomRunner.getConfig(ALL_RULES, schema);

    // Builds the relation.
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode relNode = buildComplexQuery2(builder, 1000, 1, 600, 40000, 0.01, 1000);

    // Transforms to SQL node.
    SqlImplementor.enableSelectListFix = true;
    final SqlNode sqlNode = CONVERTER.visitChild(0, relNode).asStatement();
    System.out.println("Original SQL query:");
    System.out.println(sqlNode.toSqlString(DEFAULT_DIALECT) + "\n");
    SqlImplementor.enableSelectListFix = false;

    // Transforms the relation.
    final Planner planner = Frameworks.getPlanner(config);
    final String transformed = RelCustomRunner.transformQuery(planner, sqlNode, true, true);
    System.out.println("Optimized SQL query:");
    System.out.println(transformed + "\n");

    // Closes the resources.
    dataSource.close();
    planner.close();
  }

  @Test public void complexTest3() throws Exception {
    final BasicDataSource dataSource = getDataSource();
    final SchemaPlus schema = getSchema(dataSource);
    final FrameworkConfig config = RelCustomRunner.getConfig(ALL_RULES, schema);

    // Builds the relation.
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode relNode = buildComplexQuery3(builder, 5, 40000, 1, 0.01, 7000);

    // Transforms to SQL node.
    SqlImplementor.enableSelectListFix = true;
    final SqlNode sqlNode = CONVERTER.visitChild(0, relNode).asStatement();
    System.out.println("Original SQL query:");
    System.out.println(sqlNode.toSqlString(DEFAULT_DIALECT) + "\n");
    SqlImplementor.enableSelectListFix = false;

    // Transforms the relation.
    final Planner planner = Frameworks.getPlanner(config);
    final String transformed = RelCustomRunner.transformQuery(planner, sqlNode, true, true);
    System.out.println("Optimized SQL query:");
    System.out.println(transformed + "\n");

    // Closes the resources.
    dataSource.close();
    planner.close();
  }

  @Test public void complexTest4() throws Exception {
    final BasicDataSource dataSource = getDataSource();
    final SchemaPlus schema = getSchema(dataSource);
    final FrameworkConfig config = RelCustomRunner.getConfig(ALL_RULES, schema);

    // Builds the relation.
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode relNode = buildComplexQuery4(builder, 5, 10000, 1, 0.01, 7000, 10);

    // Transforms to SQL node.
    SqlImplementor.enableSelectListFix = true;
    final SqlNode sqlNode = CONVERTER.visitChild(0, relNode).asStatement();
    System.out.println("Original SQL query:");
    System.out.println(sqlNode.toSqlString(DEFAULT_DIALECT) + "\n");
    SqlImplementor.enableSelectListFix = false;

    // Transforms the relation.
    final Planner planner = Frameworks.getPlanner(config);
    final String transformed = RelCustomRunner.transformQuery(planner, sqlNode, true, true);
    System.out.println("Optimized SQL query:");
    System.out.println(transformed + "\n");

    // Closes the resources.
    dataSource.close();
    planner.close();
  }

  @Test public void complexTest5() throws Exception {
    final BasicDataSource dataSource = getDataSource();
    final SchemaPlus schema = getSchema(dataSource);
    final FrameworkConfig config = RelCustomRunner.getConfig(ALL_RULES, schema);

    // Builds the relation.
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode relNode = buildComplexQuery5(builder, 7000, 47, 1000, 0.01, 5, 4000);

    // Transforms to SQL node.
    SqlImplementor.enableSelectListFix = true;
    final SqlNode sqlNode = CONVERTER.visitChild(0, relNode).asStatement();
    System.out.println("Original SQL query:");
    System.out.println(sqlNode.toSqlString(DEFAULT_DIALECT) + "\n");
    SqlImplementor.enableSelectListFix = false;

    // Transforms the relation.
    final Planner planner = Frameworks.getPlanner(config);
    final String transformed = RelCustomRunner.transformQuery(planner, sqlNode, true, true);
    System.out.println("Optimized SQL query:");
    System.out.println(transformed + "\n");

    // Closes the resources.
    dataSource.close();
    planner.close();
  }

  /**
   * Creates a JDBC data source for database connection.
   *
   * @return the {@link BasicDataSource} created.
   * @throws Exception when class not found.
   */
  private static BasicDataSource getDataSource() throws Exception {
    // Creates the connection to PostgreSQL server.
    Class.forName(DEFAULT_DRIVER_NAME);
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:postgresql://localhost:5432/" + DEFAULT_DATABASE);
    dataSource.setUsername(DEFAULT_USER);
    dataSource.setPassword(DEFAULT_PASSWORD);

    return dataSource;
  }

  /**
   * Creates a JDBC-based schema for a given JDBC data source.
   *
   * @param dataSource is the given JDBC data source.
   * @return the schema created.
   */
  private SchemaPlus getSchema(final BasicDataSource dataSource) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final Schema peopleSchema = JdbcSchema.create(rootSchema, DEFAULT_NAME, dataSource,
        null, DEFAULT_NAMESPACE);
    return rootSchema.add(DEFAULT_NAMESPACE, peopleSchema);
  }

  private RelNode buildComplexQuery1(final RelBuilder builder, Object... args) {
    if (args.length != 5) {
      throw new IllegalArgumentException("wrong number of arguments");
    }

    // part table.
    builder.scan("part");
    final RexNode condition1 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("p_retailprice"), builder.literal(args[0]));
    builder.filter(condition1).as("part");

    // lineitem table.
    builder.scan("lineitem");
    final RexNode condition2 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("l_quantity"), builder.literal(args[1]));
    builder.filter(condition2).as("lineitem");

    // orders table.
    builder.scan("orders");
    final RexNode condition3 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("o_totalprice"), builder.literal(args[2]));
    builder.filter(condition3).as("orders");

    // lineitem INNER JOIN orders.
    final RexNode condition4a = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "l_orderkey"),
        builder.field(2, 1, "o_orderkey"));
    final RexNode condition4b = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field(2, 0, "l_extendedprice"),
        builder.call(SqlStdOperatorTable.MULTIPLY,
            builder.field(2, 1, "o_totalprice"),
            builder.literal(args[3])));
    final RexNode condition4 = builder.and(condition4a, condition4b);
    builder.join(JoinRelType.INNER, condition4);

    // part FULL JOIN xxx.
    final RexNode condition5 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "p_partkey"),
        builder.field(2, 1, "l_partkey"));
    builder.join(JoinRelType.FULL, condition5);

    // partsupp table.
    builder.scan("partsupp");
    final RexNode condition6 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("ps_availqty"), builder.literal(args[4]));
    builder.filter(condition6).as("partsupp");

    // xxx ANTI JOIN partsupp.
    final RexNode condition7 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "p_partkey"),
        builder.field(2, 1, "ps_partkey"));
    builder.antiJoin(condition7);

    // project.
    builder.project(
        builder.field("p_name"),
        builder.field("l_suppkey"),
        builder.field("o_orderkey"),
        builder.field("o_orderstatus"));
    return builder.build();
  }

  private RelNode buildComplexQuery2(final RelBuilder builder, Object... args) {
    if (args.length != 6) {
      throw new IllegalArgumentException("wrong number of arguments");
    }

    // part table.
    builder.scan("part");
    final RexNode condition1 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("p_retailprice"), builder.literal(args[0]));
    builder.filter(condition1).as("part");

    // lineitem table.
    builder.scan("lineitem");
    final RexNode condition2 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("l_quantity"), builder.literal(args[1]));
    builder.filter(condition2).as("lineitem");

    // partsupp table.
    builder.scan("partsupp1");
    final RexNode condition3 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("ps1_supplycost"), builder.literal(args[2]));
    builder.filter(condition3).as("partsupp1");

    // lineitem INNER JOIN partsupp.
    final RexNode condition4 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "l_suppkey"),
        builder.field(2, 1, "ps1_suppkey"));
    builder.join(JoinRelType.INNER, condition4);

    // orders table.
    builder.scan("orders");
    final RexNode condition5 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("o_totalprice"), builder.literal(args[3]));
    builder.filter(condition5).as("orders");

    // XXX INNER JOIN orders.
    final RexNode condition6a = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "l_orderkey"),
        builder.field(2, 1, "o_orderkey"));
    final RexNode condition6b = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field(2, 0, "l_extendedprice"),
        builder.call(SqlStdOperatorTable.MULTIPLY,
            builder.field(2, 1, "o_totalprice"),
            builder.literal(args[4])));
    final RexNode condition6 = builder.and(condition6a, condition6b);
    builder.join(JoinRelType.INNER, condition6);

    // part FULL JOIN xxx.
    final RexNode condition7 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "p_partkey"),
        builder.field(2, 1, "l_partkey"));
    builder.join(JoinRelType.FULL, condition7);

    // partsupp table.
    builder.scan("partsupp");
    final RexNode condition8 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("ps_availqty"), builder.literal(args[5]));
    builder.filter(condition8).as("partsupp");

    // xxx ANTI JOIN partsupp.
    final RexNode condition9 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "p_partkey"),
        builder.field(2, 1, "ps_partkey"));
    builder.antiJoin(condition9);

    // project.
    builder.project(
        builder.field("p_name"),
        builder.field("l_suppkey"),
        builder.field("o_orderkey"),
        builder.field("o_orderstatus"));
    return builder.build();
  }

  private RelNode buildComplexQuery3(final RelBuilder builder, Object... args) {
    if (args.length != 5) {
      throw new IllegalArgumentException("wrong number of arguments");
    }

    // customer table.
    builder.scan("customer");
    final RexNode condition1 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("c_acctbal"), builder.literal(args[0]));
    builder.filter(condition1);

    // orders table.
    builder.scan("orders");
    final RexNode condition2 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("o_totalprice"), builder.literal(args[1]));
    builder.filter(condition2);

    // orders table.
    builder.scan("lineitem");
    final RexNode condition3 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("l_quantity"), builder.literal(args[2]));
    builder.filter(condition3);

    // lineitem INNER JOIN orders.
    final RexNode condition4a = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "o_orderkey"),
        builder.field(2, 1, "l_orderkey"));
    final RexNode condition4b = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field(2, 1, "l_extendedprice"),
        builder.call(SqlStdOperatorTable.MULTIPLY,
            builder.field(2, 0, "o_totalprice"),
            builder.literal(args[3])));
    final RexNode condition4 = builder.and(condition4a, condition4b);
    builder.join(JoinRelType.INNER, condition4);

    // customer FULL JOIN xxx.
    final RexNode condition5 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "c_custkey"),
        builder.field(2, 1, "o_custkey"));
    builder.join(JoinRelType.FULL, condition5);

    // supplier table.
    builder.scan("supplier");
    final RexNode condition6 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("s_acctbal"), builder.literal(args[4]));
    builder.filter(condition6);

    // xxx ANTI JOIN supplier.
    final RexNode condition7 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "c_nationkey"),
        builder.field(2, 1, "s_nationkey"));
    builder.antiJoin(condition7);

    // project.
    builder.project(
        builder.field("c_custkey"),
        builder.field("c_name"),
        builder.field("o_orderkey"),
        builder.field("o_orderstatus"),
        builder.field("l_linenumber"));
    return builder.build();
  }

  private RelNode buildComplexQuery4(final RelBuilder builder, Object... args) {
    if (args.length != 6) {
      throw new IllegalArgumentException("wrong number of arguments");
    }

    // customer table.
    builder.scan("customer");
    final RexNode condition1 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("c_acctbal"), builder.literal(args[0]));
    builder.filter(condition1);

    // orders table.
    builder.scan("orders");
    final RexNode condition2 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("o_totalprice"), builder.literal(args[1]));
    builder.filter(condition2);

    // orders table.
    builder.scan("lineitem");
    final RexNode condition3 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("l_quantity"), builder.literal(args[2]));
    builder.filter(condition3);

    // lineitem INNER JOIN orders.
    final RexNode condition4a = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "o_orderkey"),
        builder.field(2, 1, "l_orderkey"));
    final RexNode condition4b = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field(2, 1, "l_extendedprice"),
        builder.call(SqlStdOperatorTable.MULTIPLY,
            builder.field(2, 0, "o_totalprice"),
            builder.literal(args[3])));
    final RexNode condition4 = builder.and(condition4a, condition4b);
    builder.join(JoinRelType.INNER, condition4);

    // customer FULL JOIN xxx.
    final RexNode condition5 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "c_custkey"),
        builder.field(2, 1, "o_custkey"));
    builder.join(JoinRelType.FULL, condition5);

    // supplier table.
    builder.scan("supplier");
    final RexNode condition6 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("s_acctbal"), builder.literal(args[4]));
    builder.filter(condition6);

    // xxx ANTI JOIN supplier.
    final RexNode condition7 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "c_nationkey"),
        builder.field(2, 1, "s_nationkey"));
    builder.antiJoin(condition7);

    // part table.
    builder.scan("part");
    final RexNode condition8 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("p_size"), builder.literal(args[5]));
    builder.filter(condition8);

    // xxx LEFT JOIN part.
    final RexNode condition9 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "l_partkey"),
        builder.field(2, 1, "p_partkey"));
    builder.join(JoinRelType.LEFT, condition9);

    // project.
    builder.project(
        builder.field("c_custkey"),
        builder.field("c_name"),
        builder.field("o_orderkey"),
        builder.field("o_orderstatus"),
        builder.field("l_linenumber"),
        builder.field("p_partkey"));
    return builder.build();
  }

  private RelNode buildComplexQuery5(final RelBuilder builder, Object... args) {
    if (args.length != 6) {
      throw new IllegalArgumentException("wrong number of arguments");
    }

    // customer table.
    builder.scan("supplier");
    final RexNode condition1 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("s_acctbal"), builder.literal(args[0]));
    builder.filter(condition1);

    // lineitem table.
    builder.scan("lineitem");
    final RexNode condition2 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("l_quantity"), builder.literal(args[1]));
    builder.filter(condition2);

    // orders table.
    builder.scan("orders");
    final RexNode condition3 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("o_totalprice"), builder.literal(args[2]));
    builder.filter(condition3);

    // lineitem INNER JOIN orders.
    final RexNode condition4a = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "l_orderkey"),
        builder.field(2, 1, "o_orderkey"));
    final RexNode condition4b = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field(2, 0, "l_extendedprice"),
        builder.call(SqlStdOperatorTable.MULTIPLY,
            builder.field(2, 1, "o_totalprice"),
            builder.literal(args[3])));
    final RexNode condition4 = builder.and(condition4a, condition4b);
    builder.join(JoinRelType.INNER, condition4);

    // supplier FULL JOIN xxx.
    final RexNode condition5 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "s_suppkey"),
        builder.field(2, 1, "l_suppkey"));
    builder.join(JoinRelType.FULL, condition5);

    // customer table.
    builder.scan("customer");
    final RexNode condition6 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("c_acctbal"), builder.literal(args[4]));
    builder.filter(condition6);

    // xxx ANTI JOIN supplier.
    final RexNode condition7 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "s_nationkey"),
        builder.field(2, 1, "c_nationkey"));
    builder.antiJoin(condition7);

    // partsupp table.
    builder.scan("partsupp");
    final RexNode condition8 = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field("ps_availqty"), builder.literal(args[5]));
    builder.filter(condition8);

    // xxx LEFT JOIN partsupp.
    final RexNode condition9 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "s_suppkey"),
        builder.field(2, 1, "ps_suppkey"));
    builder.join(JoinRelType.LEFT, condition9);

    // project.
    builder.project(
        builder.field("s_suppkey"),
        builder.field("l_orderkey"),
        builder.field("l_linenumber"),
        builder.field("o_orderstatus"),
        builder.field("ps_partkey"),
        builder.field("ps_suppkey"));
    return builder.build();
  }
}

// End RunnerTPC.java
