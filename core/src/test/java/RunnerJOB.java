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

import org.apache.commons.dbcp2.BasicDataSource;

import net.jcip.annotations.NotThreadSafe;

import org.junit.Before;
import org.junit.Test;
import org.postgresql.Driver;

/**
 * A runner class for manual testing on JOB (join ordering benchmark) queries
 * with IMDB (Internet movie database) schema.
 */
@NotThreadSafe
public class RunnerJOB {
  // Default parameters for JDBC connections.
  private static final String DEFAULT_DRIVER_NAME = Driver.class.getName();
  private static final String DEFAULT_DATABASE = "imdb";
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

    // Sets a hard timeout for Volcano planner.
    VolcanoPlanner.maxTick = 100_000;
  }

  @Test public void query0() throws Exception {
    final BasicDataSource dataSource = getDataSource();
    final SchemaPlus schema = getSchema(dataSource);

    final String sqlQuery = "SELECT * FROM (\n"
        + "    SELECT * FROM \n"
        + "        (SELECT * FROM company_type WHERE kind = 'production companies') AS ct\n"
        + "    FULL JOIN (\n"
        + "        SELECT * FROM (\n"
        + "            SELECT mc.note AS mc_note, \n"
        + "                   mc.movie_id AS mc_movie_id, \n"
        + "                   mc.company_type_id AS mc_company_type_id,\n"
        + "                   mi_idx.movie_id AS mi_idx_movie_id,\n"
        + "                   mi_idx.info_type_id AS mi_idx_info_type_id FROM \n"
        + "                movie_companies AS mc\n"
        + "            INNER JOIN\n"
        + "                movie_info_idx AS mi_idx \n"
        + "            ON\n"
        + "                mc.movie_id = mi_idx.movie_id\n"
        + "            WHERE\n"
        + "                mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%' \n"
        + "                AND\n"
        + "                (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%')\n"
        + "            ) AS r0\n"
        + "        INNER JOIN\n"
        + "            (SELECT id, title AS t_title, production_year AS t_production_year FROM title) AS t\n"
        + "        ON t.id = r0.mc_movie_id AND t.id = r0.mi_idx_movie_id\n"
        + "    ) AS r1\n"
        + "    ON ct.id = r1.mc_company_type_id\n"
        + ") AS r2 INNER JOIN (\n"
        + "    SELECT * FROM info_type WHERE info = 'top 250 rank'\n"
        + ") AS it\n"
        + "ON r2.mi_idx_info_type_id = it.id";
    RelCustomRunner.buildAndTransformQuery(ALL_RULES, schema, sqlQuery, true, true);
  }

  @Test public void query1() throws Exception {
    final BasicDataSource dataSource = getDataSource();
    final SchemaPlus schema = getSchema(dataSource);
    final FrameworkConfig config = RelCustomRunner.getConfig(ALL_RULES, schema);

    // Builds the relation.
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode relNode = buildQuery1(builder);

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

  /**
   * Constructs a {@link RelNode} for query 1.
   *
   * @param builder is the {@link RelBuilder}.
   * @param args are the input parameters for this query.
   * @return the created {@link RelNode}.
   */
  private RelNode buildQuery1(final RelBuilder builder, final Object... args) {
    // company_type table.
    builder.scan("company_type");
    final RexNode condition1 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field("kind"),
        builder.literal("production companies"));
    builder.filter(condition1);

    // movie_companies table.
    builder.scan("movie_companies");
    final RexNode condition2a = builder.call(SqlStdOperatorTable.NOT_LIKE,
        builder.field("note"),
        builder.literal("%(as Metro-Goldwyn-Mayer Pictures)%"));
    final RexNode condition2b = builder.call(SqlStdOperatorTable.LIKE,
        builder.field("note"),
        builder.literal("%(co-production)%"));
    final RexNode condition2c = builder.call(SqlStdOperatorTable.LIKE,
        builder.field("note"),
        builder.literal("%(presents)%"));
    final RexNode condition2d = builder.or(condition2b, condition2c);
    final RexNode condition2 = builder.and(condition2a, condition2d);
    builder.filter(condition2);

    // movie_info_idx table.
    builder.scan("movie_info_idx");

    // movie_companies INNER JOIN movie_info_idx.
    final RexNode condition3 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "movie_id"),
        builder.field(2, 1, "movie_id"));
    builder.join(JoinRelType.INNER, condition3);

    // title table.
    builder.scan("title");

    // xxx INNER JOIN title.
    final RexNode condition4a = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, 1),
        builder.field(2, 1, "id"));
    final RexNode condition4b = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, 6),
        builder.field(2, 1, "id"));
    final RexNode condition4 = builder.and(condition4a, condition4b);
    builder.join(JoinRelType.INNER, condition4);

    // company_type FULL JOIN xxx.
    final RexNode condition5 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "id"),
        builder.field(2, 1, "company_type_id"));
    builder.join(JoinRelType.FULL, condition5);

    // info_type table.
    builder.scan("info_type");
    final RexNode condition6 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field("info"),
        builder.literal("top 250 rank"));
    builder.filter(condition6);

    // xxx ANTI JOIN info_type.
    final RexNode condition7 = builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(2, 0, "info_type_id"),
        builder.field(2, 1, "id"));
    builder.antiJoin(condition7);

    // Constructs the final query.
    builder.project(
        builder.field(0),   // company_type.id
        builder.field(6),   // movie_companies.note
        builder.field(8),   // movie_info_idx.movie_id
        builder.field(13)); // title.title
    return builder.build();
  }
}

// End RunnerJOB.java
