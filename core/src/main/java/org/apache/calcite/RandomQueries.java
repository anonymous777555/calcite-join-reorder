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
import org.apache.calcite.adapter.java.ReflectiveSchema;
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
import org.apache.calcite.rel.rules.custom.AsscomInnerLeftRule;
import org.apache.calcite.rel.rules.custom.AsscomLeftInnerRule;
import org.apache.calcite.rel.rules.custom.AsscomLeftLeftRule;
import org.apache.calcite.rel.rules.custom.AssocGeneralRule;
import org.apache.calcite.rel.rules.custom.AssocLeftInnerRule;
import org.apache.calcite.rel.rules.custom.AssocReverseInnerLeftRule;
import org.apache.calcite.rel.rules.custom.BestMatchNullifyPullUpRule;
import org.apache.calcite.rel.rules.custom.BestMatchReduceRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * A main driver class to test calcite-core against some random generated queries.
 */
public class RandomQueries {
  //~ Static fields/initializers ---------------------------------------------

  // A default random generator.
  private static final Random RANDOM_GENERATOR = new Random();

  // Defines the default dialect used for output query in this class.
  public static final SqlDialect DEFAULT_DIALECT
      = SqlDialect.DatabaseProduct.MYSQL.getDialect();

  // The default schema and the object to be used.
  private static final String DEFAULT_NAMESPACE = "public";
  private static final Object DEFAULT_SCHEMA_OBJECT = new RandomSchema();
  private static final Schema DEFAULT_SCHEMA = new ReflectiveSchema(DEFAULT_SCHEMA_OBJECT);

  // All join types that we would like to consider.
  private static final List<JoinRelType> ALL_JOIN_TYPES = ImmutableList.of(
      JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.FULL, JoinRelType.ANTI
  );

  // All SQL operators that we would like to consider in join predicates.
  private static final List<SqlOperator> ALL_OPERATORS = ImmutableList.of(
      SqlStdOperatorTable.EQUALS, SqlStdOperatorTable.NOT_EQUALS,
      SqlStdOperatorTable.GREATER_THAN, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      SqlStdOperatorTable.LESS_THAN, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);

  // The maximum number of conjuncts in each join predicate.
  private static final int MAX_CONJUNCT = 2;

  // All tables that we would like to consider.
  private static final Set<String> ALL_TABLES = ImmutableSet.of(
      "a", "b", "c", "d", "e", "f", "g", "h"
  );

  // The upper limit to add selectivity for different tables.
  private static final Map<String, Integer> PER_TABLE_UPPER_LIMIT = ImmutableMap
      .<String, Integer>builder()
      .put("a", 1_000)
      .put("b", 1_000)
      .put("c", 10_000)
      .put("d", 10_000)
      .put("e", 100_000)
      .put("f", 100_000)
      .build();

  // All rules that we would like to consider.
  private static final Program ALL_RULES = Programs.ofRules(
      // All enumerable rules.
      EnumerableRules.ENUMERABLE_BEST_MATCH_RULE,
      EnumerableRules.ENUMERABLE_JOIN_RULE,
      EnumerableRules.ENUMERABLE_NULLIFY_RULE,
      EnumerableRules.ENUMERABLE_PROJECT_RULE,
      EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,

      // All our custom rules.
      AsscomGeneralLeftRule.INSTANCE,
      AsscomGeneralRightRule.INSTANCE,
      AsscomInnerLeftRule.INSTANCE,
      AsscomLeftInnerRule.INSTANCE,
      AsscomLeftLeftRule.INSTANCE,
      AssocGeneralRule.INSTANCE,
      AssocReverseInnerLeftRule.INSTANCE,
      AssocLeftInnerRule.INSTANCE,

      // Pull up best-match / nullification / projection.
      BestMatchNullifyPullUpRule.LEFT_CHILD,
      BestMatchNullifyPullUpRule.RIGHT_CHILD,
      BestMatchReduceRule.INSTANCE,
      JoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER,
      JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER,
      JoinProjectTransposeRule.BOTH_PROJECT_INCLUDE_OUTER
  );

  // The batch size for progress bar.
  private static final int BATCH_SIZE = 10;

  // The hard limit for the number of queries in the current round.
  private static final int QUERY_LIMIT = 20_000;

  // The prefix offset for the queries.
  private static final int QUERY_OFFSET = 0;

  // A feature flag to determine whether we should add selectivity on top of table scan.
  public static boolean enableSingleTableSelect = false;

  //~ Methods ----------------------------------------------------------------

  public static void main(String[] args) throws Exception {
    // Validates all parameters passed as CLI arguments.
    if (args.length < 2) {
      System.err.println("Usage: mvn exec:java <num_nodes> <node_id>");
      return;
    }
    final int numNodes = Integer.parseInt(args[0]);
    final int nodeID = Integer.parseInt(args[1]);
    final int queryLimit = QUERY_LIMIT * numNodes + QUERY_OFFSET;
    if (numNodes < 1) {
      System.err.println("There should be at least 1 node running.");
      return;
    } else if (nodeID < 0 || nodeID >= numNodes) {
      System.err.println("The ID of current node should be within [0, num_nodes).");
      return;
    }
    System.out.println("Going to run #" + nodeID + " out of " + numNodes + " nodes ...");

    // Builds the root schema.
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus defaultSchema = rootSchema.add(DEFAULT_NAMESPACE, DEFAULT_SCHEMA);
    final FrameworkConfig config = RelCustomRunner.getConfig(ALL_RULES, defaultSchema);

    // Collects all possible queries.
    System.out.println("\n1. Generating all possible queries ...");
    final RelBuilder builder = RelBuilder.create(config);
    final Set<RelNode> relNodes = generateRandomQueries(builder, ALL_TABLES, queryLimit);

    // Initializes the dependencies.
    System.out.println("\n2. Processing all generated queries ...");
    VolcanoPlanner.maxTick = 10_000;
    final RelToSqlConverter converter = new RelToSqlConverter(DEFAULT_DIALECT);
    final Planner planner = Frameworks.getPlanner(config);

    // Initializes the output stream.
    final String outputFile = "output_" + nodeID + ".txt";
    final Path outputFilePath = Paths.get(outputFile);
    final BufferedWriter writer = Files.newBufferedWriter(outputFilePath);

    // Processes each query.
    int count = 0;
    int lastPrintCount = 0;
    for (final RelNode relNode: relNodes) {
      // Skips within the given offset or the current node belongs to a different partition.
      count++;
      if (count < QUERY_OFFSET || count % numNodes != nodeID) {
        continue;
      }

      // Converts to SQL node format.
      SqlImplementor.enableSelectListFix = true;
      final SqlNode sqlNode = converter.visitChild(0, relNode).asStatement();
      SqlImplementor.enableSelectListFix = false;

      // Transforms the query.
      try {
        RelCustomRunner.transformQuery(planner, sqlNode);
      } catch (Exception e) {
        System.err.println("Unable to transform query: " + sqlNode);
        e.printStackTrace();
      }
      planner.close();

      // Outputs the result.
      RelOptCustomUtil.outputTransformationPairs(writer, sqlNode);

      // Updates the progress bar in console (if necessary).
      if (count >= lastPrintCount + BATCH_SIZE) {
        System.out.println("Progress: " + count + " out of " + relNodes.size());
        lastPrintCount = count;
      }

      // Stops earlier (if necessary).
      if (count > queryLimit) {
        System.out.println("Stopped earlier as we have reached the upper limit of " + queryLimit);
        break;
      }
    }

    // Outputs the overall statistics and closes the output stream.
    RelOptCustomUtil.outputStatistics();
    writer.flush();
    writer.close();
  }

  /**
   * Generates random queries based on different join ordering. By join ordering, here
   * we consider all types (including left deep tree, right deep tree, bushy tree). In
   * addition, this method returns a deterministic set of queries even though we say it
   * is "random".
   *
   * @param builder is the {@link RelBuilder} used to construct the {@link RelNode}.
   * @param tables is the names of all tables.
   * @param limit is the upper limit for the number of queries to be generated.
   * @return a set of random queries generated.
   */
  public static Set<RelNode> generateRandomQueries(final RelBuilder builder,
      final Set<String> tables, final int limit) {
    // Computes all permutations of the tables.
    final Collection<List<String>> allOrdering = Collections2.permutations(tables);

    // Generates all possible shapes for each given ordering.
    final Set<RelNode> result = new HashSet<>();
    for (final List<String> ordering: allOrdering) {
      final List<Pair<RelNode, List<String>>> shapes = generateAllJoinShapes(builder, ordering);
      final List<RelNode> queries = Pair.left(shapes);
      result.addAll(queries);

      // Stops earlier if we have reached the upper limit.
      if (result.size() > limit) {
        break;
      }
    }
    return result;
  }

  /**
   * Generates all possible join shapes based on a given join ordering.
   *
   * @param builder is the {@link RelBuilder} used to construct the {@link RelNode}.
   * @param ordering is the required ordering of the tables.
   * @return a list of random queries generated.
   */
  private static List<Pair<RelNode, List<String>>> generateAllJoinShapes(final RelBuilder builder,
      final List<String> ordering) {
    if (ordering.size() == 1) {
      final String tableName = ordering.get(0);
      final RelNode node = generateSingleTable(builder, tableName);
      return ImmutableList.of(Pair.of(node, ImmutableList.of(tableName)));
    }

    // Splits the tables on different points.
    List<Pair<RelNode, List<String>>> result = new ArrayList<>();
    for (int i = 1; i < ordering.size(); i++) {
      // Splits into 2 parts.
      final List<String> leftTables = ordering.subList(0, i);
      final List<String> rightTables = ordering.subList(i, ordering.size());

      // Generates shapes for left and right side recursively.
      final List<Pair<RelNode, List<String>>> leftShapes =
          generateAllJoinShapes(builder, leftTables);
      final List<Pair<RelNode, List<String>>> rightShapes =
          generateAllJoinShapes(builder, rightTables);

      // Joins the 2 parts.
      for (final Pair<RelNode, List<String>> leftShape: leftShapes) {
        for (final Pair<RelNode, List<String>> rightShape: rightShapes) {
          for (final JoinRelType joinType: ALL_JOIN_TYPES) {
            builder.push(leftShape.left).push(rightShape.left);

            // Builds the join predicate based on two sides.
            final List<String> leftList = leftShape.right;
            final List<String> rightList = rightShape.right;
            final RexNode joinPredicate = getRandomJoinPredicate(builder, leftList, rightList);

            // Builds the new list.
            final List<String> newList = new ArrayList<>(leftList);
            if (joinType.projectsRight()) {
              newList.addAll(rightList);
            }
            final RelNode shape = builder.join(joinType, joinPredicate).build();
            result.add(Pair.of(shape, newList));
          }
        }
      }
    }
    return result;
  }

  /**
   * Generates a {@link RelNode} to query from the given base relation.
   *
   * @param builder is the {@link RelBuilder}.
   * @param tableName is the name of the base relation.
   * @return the generated {@link RelNode}.
   */
  private static RelNode generateSingleTable(final RelBuilder builder, final String tableName) {
    builder.scan(tableName);
    if (!enableSingleTableSelect) {
      return builder.build();
    }

    final RexNode predicate = builder.call(SqlStdOperatorTable.LESS_THAN,
        builder.field(getColumnName(tableName)),
        builder.literal(PER_TABLE_UPPER_LIMIT.get(tableName)));
    return builder.filter(predicate).build();
  }

  /**
   * Gets a random {@link RexNode} as the join predicate, by connecting multiple
   * conjuncts using AND or OR.
   *
   * @param builder is the {@link RelBuilder}.
   * @param leftTables are the names of left tables.
   * @param rightTables are the names of the right tables.
   * @return the random predicate.
   */
  private static RexNode getRandomJoinPredicate(final RelBuilder builder,
      final List<String> leftTables, final List<String> rightTables) {
    // Computes the number of conjuncts suitable for the current node.
    final int maxPairs = leftTables.size() * rightTables.size();
    final int maxConjuncts = Math.min(MAX_CONJUNCT, maxPairs);
    final int numConjuncts = RANDOM_GENERATOR.nextInt(maxConjuncts) + 1;
    final List<RexNode> operands = new ArrayList<>(numConjuncts);

    // Generates all possible pairs of tables to be referred by each conjunct.
    final List<String> shuffleLeft = new ArrayList<>(leftTables);
    Collections.shuffle(shuffleLeft, RANDOM_GENERATOR);
    final List<String> shuffleRight = new ArrayList<>(rightTables);
    Collections.shuffle(shuffleRight, RANDOM_GENERATOR);

    // Makes sure the pairs of tables referred by each conjunct is always distinct.
    for (int i = 0; i < shuffleLeft.size() && operands.size() < numConjuncts; i++) {
      for (int j = 0; j < shuffleRight.size() && operands.size() < numConjuncts; j++) {
        final String leftTable = shuffleLeft.get(i);
        final String rightTable = shuffleRight.get(j);

        // Constructs a conjunct.
        final RexNode operand = getRandomConjunct(builder, leftTable, rightTable);
        operands.add(operand);
      }
    }

    // Connects them using AND. We do not use OR for now because that will result in
    // null-tolerant predicate(s).
    return builder.and(operands);
  }

  /**
   * Gets a random {@link RexNode} as a conjunct in a join predicate.
   *
   * @param builder is the {@link RelBuilder}.
   * @param left is the name of left table.
   * @param right is the name of the right table.
   * @return the random predicate.
   */
  private static RexNode getRandomConjunct(RelBuilder builder, String left, String right) {
    final String leftName = getColumnName(left);
    final RexInputRef leftField = builder.field(2, 0, leftName);
    final String rightName = getColumnName(right);
    final RexInputRef rightField = builder.field(2, 1, rightName);
    return builder.call(getRandomOperator(), leftField, rightField);
  }

  private static String getColumnName(final String tableName) {
    return tableName.substring(0, 1) + "ID";
  }

  /**
   * @return a random SQL operator.
   */
  private static SqlOperator getRandomOperator() {
    int i = RANDOM_GENERATOR.nextInt(ALL_OPERATORS.size());
    return ALL_OPERATORS.get(i);
  }

  private RandomQueries() {
  }
}

// End RandomQueries.java
