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
package org.apache.calcite.rel;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptCustomUtil;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.BestMatch;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;

import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * A utility class for running a query.
 */
public class RelCustomRunner {
  //~ Static fields/initializers ---------------------------------------------

  // Defines the default lex used for input query in this class.
  private static final Lex DEFAULT_LEX = Lex.MYSQL;
  // Defines the default parser configuration used for input query in this class.
  private static final SqlParser.Config PARSER_CONFIG
      = SqlParser.configBuilder().setLex(DEFAULT_LEX).build();

  private static final String EXTERNAL_DELIMITER
      = "=============================================================\n";
  private static final String INTERNAL_DELIMITER
      = "-------------------------------------------------------------\n";

  // A counter for the number of transactions executed so far.
  private static int count = 1;

  // A few counters for statistics on whether compensation operators are on top of the query plan.
  private static int total = 0;
  private static int compensationTotal = 0;
  private static int compensationOnTopTotal = 0;
  private static List<String> compensationNotOnTop = new ArrayList<>();

  //~ Methods ----------------------------------------------------------------

  /**
   * Emulates the whole life cycle of a given SQL query: parse, validate build and
   * transform. It will close the planner after usage. It disables type check by default.
   *
   * @param programs is the set of transformation rules to be used.
   * @param schema is the schema for the query to be executed in.
   * @param sqlQuery is the original SQL query in its string representation.
   * @throws Exception when there is error during any step.
   */
  public static void buildAndTransformQuery(final Program programs, final SchemaPlus schema,
      final String sqlQuery) throws Exception {
    buildAndTransformQuery(programs, schema, sqlQuery, true);
  }

  /**
   * Emulates the whole life cycle of a given SQL query: parse, validate build and
   * transform. It will close the planner after usage. It does not use JDBC by default.
   *
   * @param programs is the set of transformation rules to be used.
   * @param schema is the schema for the query to be executed in.
   * @param sqlQuery is the original SQL query in its string representation.
   * @throws Exception when there is error during any step.
   */
  public static void buildAndTransformQuery(final Program programs, final SchemaPlus schema,
      final String sqlQuery, final boolean ignoreTypeCheck) throws Exception {
    buildAndTransformQuery(programs, schema, sqlQuery, ignoreTypeCheck, false);
  }

  /**
   * Emulates the whole life cycle of a given SQL query: parse, validate build and
   * transform. It will close the planner after usage.
   *
   * @param programs is the set of transformation rules to be used.
   * @param schema is the schema for the query to be executed in.
   * @param sqlQuery is the original SQL query in its string representation.
   * @param ignoreTypeCheck indicates whether type check should be turned off.
   * @throws Exception when there is error during any step.
   */
  public static void buildAndTransformQuery(
      final Program programs, final SchemaPlus schema, final String sqlQuery,
      final boolean ignoreTypeCheck, final boolean useJDBC) throws Exception {
    // Creates the planner.
    final FrameworkConfig config = getConfig(programs, schema);
    final Planner planner = Frameworks.getPlanner(config);

    System.out.println("============================ Start ============================");
    System.out.println("Transaction ID: " + count++ + "\n");

    // Prints the original SQL query string.
    System.out.println("Input query:");
    System.out.println(sqlQuery + "\n");

    // Parses, validates the query.
    final SqlNode parse = planner.parse(sqlQuery);
    final SqlNode validate = planner.validate(parse);

    // Transforms the query.
    final String outputQuery = transformQuery(planner, validate, ignoreTypeCheck, false, useJDBC);
    System.out.println("Output query:");
    System.out.println(outputQuery + "\n");
    System.out.println("============================= End =============================\n");

    // Closes the planner.
    planner.close();
  }

  public static void transformQuery(final Planner planner, final SqlNode sqlNode) throws Exception {
    planner.makeReadyForRel();

    final SqlNode validate = planner.validate(sqlNode);
    transformQuery(planner, validate, true, false);
  }

  /**
   * Transforms a given query using the given planner.
   *
   * @param planner is the {@link Planner} to be used.
   * @param sqlNode is the root node of the given query plan.
   * @throws Exception if there is any error.
   */
  public static String transformQuery(final Planner planner, final SqlNode sqlNode,
      final boolean silent, final boolean useJDBC) throws Exception {
    planner.makeReadyForRel();

    final SqlNode validate = planner.validate(sqlNode);
    return transformQuery(planner, validate, true, silent, useJDBC);
  }

  /**
   * Transforms a given query using the given planner.
   *
   * @param planner is the {@link Planner} to be used.
   * @param sqlNode is the root node of the given query plan.
   * @param ignoreTypeCheck indicates whether type check should be turned off.
   * @param silent indicates whether there should be log printed.
   * @throws Exception if there is any error.
   */
  private static String transformQuery(
      final Planner planner, final SqlNode sqlNode, final boolean ignoreTypeCheck,
      final boolean silent, final boolean useJDBC) throws Exception {
    if (ignoreTypeCheck) {
      RelOptUtil.disableTypeCheck = true;
    }
    final RelNode relNode = planner.rel(sqlNode).rel;
    if (ignoreTypeCheck) {
      RelOptUtil.disableTypeCheck = false;
    }
    if (!silent) {
      System.out.println("Before transformation:");
      System.out.println(RelOptUtil.toString(relNode));
    }

    // Transforms the query.
    if (ignoreTypeCheck) {
      RelOptUtil.disableTypeCheck = true;
    }
    RelTraitSet traitSet = relNode.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelOptRuleCall.transformCount = 0;
    RelNode transformedNode = planner.transform(0, traitSet, relNode);
    if (ignoreTypeCheck) {
      RelOptUtil.disableTypeCheck = false;
    }

    // Prints out the number of transformations performed.
    System.out.println("# of transformations performed: " + RelOptRuleCall.transformCount);

    // Prints out the transformed query.
    if (!silent) {
      System.out.println("After transformation:");
      System.out.println(RelOptUtil.toString(transformedNode));
    }

    // Converts the transformed relational expression back to SQL query string.
    final String query = RelOptCustomUtil.toQueryString(transformedNode, false, useJDBC);

    // Updates the compensation statistics.
    total++;
    if (RelOptUtil.containsClass(transformedNode, BestMatch.class)) {
      compensationTotal++;

      if (isCompensationOnTop(transformedNode)) {
        compensationOnTopTotal++;
      } else {
        compensationNotOnTop.add(query);
      }
    }
    return query;
  }

  /**
   * Creates a new {@link FrameworkConfig}.
   *
   * @param programs is the set of transformation rules to be used.
   * @param schema is the schema for the query to be executed in.
   * @return a new {@link FrameworkConfig}.
   */
  public static FrameworkConfig getConfig(final Program programs, final SchemaPlus schema) {
    return Frameworks.newConfigBuilder()
        .parserConfig(PARSER_CONFIG)
        .defaultSchema(schema)
        .programs(programs)
        .build();
  }

  /**
   * Outputs the statistics information.
   */
  public static void outputStatistics(final Writer writer) throws Exception {
    writer.write("Out of " + total + " queries, there are "
        + compensationTotal + " with compensation operators, and "
        + compensationOnTopTotal + " with compensation operators on top.\n");
  }

  /**
   * Prints all queries with compensation operators not on top.
   *
   * @param writer is the output writer to use.
   */
  public static void outputNotOnTop(final Writer writer) throws Exception {
    writer.write(EXTERNAL_DELIMITER);
    writer.write("Queries with compensation operators not on top:\n");
    writer.write(INTERNAL_DELIMITER);

    for (String query: compensationNotOnTop) {
      writer.write(query + "\n");
      writer.write(INTERNAL_DELIMITER);
    }
    writer.write(EXTERNAL_DELIMITER);
    compensationNotOnTop.clear();
  }

  /**
   * Checks whether compensation operators are on top of a given relational expression.
   *
   * @param root is the root of the relational expression.
   * @return true if compensation operators are on top of the given node.
   */
  private static boolean isCompensationOnTop(final RelNode root) {
    if (root instanceof Project) {
      return isCompensationOnTop(root.getInput(0));
    } else {
      return root instanceof BestMatch;
    }
  }

  private RelCustomRunner() {
  }
}

// End RelCustomRunner.java
