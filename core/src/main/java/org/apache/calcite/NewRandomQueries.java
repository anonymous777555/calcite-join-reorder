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
package org.apache.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptCustomUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCustomRunner;
import org.apache.calcite.rel.RelNode;
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
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.JoinScope;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import org.apache.commons.dbcp2.BasicDataSource;

import com.google.common.collect.ImmutableSet;

import org.postgresql.Driver;

import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Set;

/**
 * NewRandomQueries is a new runner class based on {@link RandomQueries}.
 */
public class NewRandomQueries {
  // Default parameters for JDBC connections.
  private static final String DEFAULT_DRIVER_NAME = Driver.class.getName();
  private static final String DEFAULT_DATABASE = "random";
  private static final String DEFAULT_USER = "calcite";
  private static final String DEFAULT_PASSWORD = "calcite";

  // The default namespace to be used.
  private static final String DEFAULT_NAME = "db";
  private static final String DEFAULT_NAMESPACE = "public";

  // Defines the default dialect used for output query in this class.
  private static final SqlDialect DEFAULT_DIALECT
      = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
  private static final RelToSqlConverter CONVERTER = new RelToSqlConverter(DEFAULT_DIALECT);

  // All tables that we would like to consider.
  private static final Set<String> ALL_TABLES = ImmutableSet.of(
      "a", "b", "c", "d", "e", "f"
  );

  // The hard limit for the number of queries in the current round.
  private static final int QUERY_LIMIT = 20_000;
  private static final int BATCH_SIZE = 100;

  // The default output path.
  private static final String OUTPUT_PATH = "output_%d.txt";
  private static final String ERR_PATH = "error_%d.txt";
  private static final String STATS_PATH = "stats_%d.txt";
  private static final String NOT_TOP_PATH = "not_on_top_%d.txt";

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

  public static void main(String[] args) throws Exception {
    // Validates all parameters passed as CLI arguments.
    if (args.length < 2) {
      System.err.println("Usage: mvn exec:java <num_nodes> <node_id> [<log_directory>]");
      return;
    }
    final int numNodes = Integer.parseInt(args[0]);
    final int nodeID = Integer.parseInt(args[1]);
    final String logDir = args.length >= 3 ? args[2] : "log";
    if (nodeID < 0 || nodeID >= numNodes) {
      System.err.println("Usage: node_id must be within [0, num_nodes).");
      return;
    }

    // Runs the program.
    System.out.println("Going to run #" + nodeID + " out of " + numNodes + " nodes ...");
    final NewRandomQueries runner = new NewRandomQueries();
    runner.setUp();
    runner.run(numNodes, nodeID, logDir);
  }

  private void setUp() {
    // Disables all assertions.
    getClass().getClassLoader().setDefaultAssertionStatus(false);

    // Encourages relations with none convention.
    VolcanoPlanner.encourageNoneConvention = true;

    // Sets a maximum limit for Volcano planner.
    VolcanoPlanner.maxTick = 30_000;

    // Encourages JDBC conversion.
    RelOptCustomUtil.defaultPreferJDBC = true;

    // Enables the fix for join scope.
    JoinScope.enableJoinScopeFix = true;

    // Enables the select on single table scan.
    RandomQueries.enableSingleTableSelect = true;
  }

  private void run(final int numNode, final int nodeID, final String logDir) throws Exception {
    final BasicDataSource dataSource = getDataSource();
    final SchemaPlus schema = getSchema(dataSource);
    final FrameworkConfig config = RelCustomRunner.getConfig(ALL_RULES, schema);
    final Planner planner = Frameworks.getPlanner(config);

    // Initializes all writers.
    final Writer outWriter = Files.newBufferedWriter(
        Paths.get(logDir, String.format(Locale.ROOT, OUTPUT_PATH, nodeID)));
    final Writer errInWriter = Files.newBufferedWriter(
        Paths.get(logDir, String.format(Locale.ROOT, ERR_PATH, nodeID)));
    final PrintWriter errWriter = new PrintWriter(errInWriter);
    final Writer statsWriter = Files.newBufferedWriter(
        Paths.get(logDir, String.format(Locale.ROOT, STATS_PATH, nodeID)));
    final Writer notTopWriter = Files.newBufferedWriter(
        Paths.get(logDir, String.format(Locale.ROOT, NOT_TOP_PATH, nodeID)));

    // Builds all the relations.
    final RelBuilder builder = RelBuilder.create(config);
    final int queryLimit = (int) (QUERY_LIMIT * 1.2);
    final Set<RelNode> rel = RandomQueries.generateRandomQueries(builder, ALL_TABLES, queryLimit);

    // Transforms each relation.
    final int actualLimit = QUERY_LIMIT / numNode;
    int count = 0;
    int actualCount = 0;
    int failureCount = 0;
    for (final RelNode node: rel) {
      // Stops if already reach the hard limit.
      if (actualCount - failureCount >= actualLimit) {
        break;
      }
      count++;

      // Only runs the query if belongs to the current partition.
      if (count % numNode != nodeID) {
        continue;
      }

      if (actualCount % BATCH_SIZE == 0) {
        RelCustomRunner.outputNotOnTop(notTopWriter);
        RelCustomRunner.outputStatistics(statsWriter);
        statsWriter.write("Out of " + actualCount + " queries, there are "
            + failureCount + " failures.\n\n");
        statsWriter.flush();
      }

      // Transforms to SQL node.
      SqlImplementor.enableSelectListFix = true;
      final SqlNode sqlNode = CONVERTER.visitChild(0, node).asStatement();
      SqlImplementor.enableSelectListFix = false;

      // Transforms the relation.
      String transformed = "";
      try {
        transformed = RelCustomRunner.transformQuery(planner, sqlNode, true, true);
      } catch (Throwable e) {
        errWriter.write("=========================\n");
        errWriter.write("Query count: #" + actualCount + "\n");
        errWriter.write("=========================\n");
        e.printStackTrace(errWriter);
        errWriter.write("=========================\n");
        failureCount++;
      }
      actualCount++;

      // Outputs the queries and closes the planner.
      final String origin = sqlNode.toSqlString(DEFAULT_DIALECT).getSql();
      RelOptCustomUtil.outputTransformationPair(outWriter, actualCount, origin, transformed);
      planner.close();
    }

    // Outputs the statistics.
    RelCustomRunner.outputNotOnTop(notTopWriter);
    RelCustomRunner.outputStatistics(statsWriter);
    statsWriter.write("Out of " + actualCount + " queries, there are "
        + failureCount + " failures.\n\n");

    // Closes the resources.
    dataSource.close();
    outWriter.flush();
    outWriter.close();
    errWriter.flush();
    errWriter.close();
    statsWriter.flush();
    statsWriter.close();
    notTopWriter.flush();
    notTopWriter.close();
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
}

// End NewRandomQueries.java
