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
package org.apache.calcite.plan;

import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Nullify;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.graph.AcyclicDirectedGraph;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * {@link RelOptCustomUtil} defines the additional static utility methods (on top of those
 * from {@link RelOptUtil}) for use in optimizing {@link RelNode}s.
 */
public abstract class RelOptCustomUtil {
  //~ Static fields/initializers ---------------------------------------------

  // A feature flag to determine whether we are in favor of JDBC implementor.
  public static boolean defaultPreferJDBC = false;

  // Defines the output dialect in this class.
  private static final SqlDialect OUTPUT_DIALECT
      = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();

  /**
   * A flag to control whether we should enable the fix to the algorithm when computing
   * nullification sets (i.e., include all tables referred by the join predicate).
   */
  private static final boolean ENABLE_NULLIFICATION_SET_FIX = true;

  /**
   * A flag to determine whether we shall enable the transformation recording.
   */
  private static final boolean ENABLE_TRANSFORMATION_RECORDING = false;

  /**
   * A map of all pairs of SQL queries that go through valid transformation.
   */
  private static Map<Pair<String, String>, String> transformationPairs = new HashMap<>();

  // The default delimiter used in result output.
  private static final String PAIR_DELIMITER
      = "=============================================================\n";
  private static final String INTERNAL_DELIMITER
      = "-------------------------------------------------------------\n";

  // A map to track statistics about the number of sorting required in best-match.
  private static final Map<Integer, Integer> STATISTICS = new TreeMap<>();

  //~ Methods ----------------------------------------------------------------

  /**
   * Generates the favorable orderings for a given query plan.
   *
   * @param root is the root node of the given query plan.
   * @return a list of all favorable orderings.
   */
  public static List<List<String>> generateFavorableOrdering(final RelNode root) {
    // Derives the nullification set for each relation in the query plan.
    final Map<String, Set<RexNode>> nullificationSetMap = deriveNullificationSets(root);

    // Finds out all tables that are being further nullified.
    final Map<String, Set<RexNode>> nullifiedSetMap = new HashMap<>();
    final Set<String> nullifiedTables = deriveNullificationTables(root);
    nullifiedTables.forEach(table -> nullifiedSetMap.put(table, nullificationSetMap.get(table)));

    // Constructs a DAG and computes the minimum path cover.
    final AcyclicDirectedGraph graph = buildDAG(nullifiedSetMap);
    final Collection<List<String>> paths = graph.minimumPathCover();

    // Updates the statistics.
    final int originCount = STATISTICS.getOrDefault(paths.size(), 0);
    STATISTICS.put(paths.size(), originCount + 1);

    // Generates the favorable ordering for each path in the minimum path cover.
    return paths.stream()
        .map(path -> composeOrderingFromPath(path, nullificationSetMap))
        .collect(Collectors.toList());
  }

  /**
   * Constructs a {@link AcyclicDirectedGraph} from given nullification sets.
   *
   * @param nullificationSets is a map from table names to their nullification sets.
   * @return a DAG of these nullification sets.
   */
  private static AcyclicDirectedGraph buildDAG(final Map<String, Set<RexNode>> nullificationSets) {
    AcyclicDirectedGraph graph = AcyclicDirectedGraph.create();

    // Adds all vertices into the graph first.
    for (final String table: nullificationSets.keySet()) {
      graph.addVertex(table);
    }

    // Adds all edges into the graph then.
    for (final String tableA: nullificationSets.keySet()) {
      final Set<RexNode> setA = nullificationSets.get(tableA);

      for (final String tableB: nullificationSets.keySet()) {
        // Skips if the same table.
        if (tableA.equals(tableB)) {
          continue;
        }
        final Set<RexNode> setB = nullificationSets.get(tableB);

        // Adds edge B -> A in the following 2 cases:
        // 1. A is the strict superset of B; or
        // 2. A is the same as B, but A's name is alphabetically larger than B.
        if (isSuperSet(setA, setB) && (tableA.compareTo(tableB) > 0 || !setA.equals(setB))) {
          graph.addEdge(tableB, tableA);
        }
      }
    }

    return graph;
  }

  /**
   * Composes the finalized favorable ordering from a given path which includes all
   * tables that have been further nullified. The other non-nullified tables will be
   * inserted at the correct position(s).
   *
   * @param path is the path for all nullified tables.
   * @param nullificationSetMap is a mapping from table name to its nullification set.
   * @return a list of table names to represent the composed favorable ordering.
   */
  private static List<String> composeOrderingFromPath(final List<String> path,
      final Map<String, Set<RexNode>> nullificationSetMap) {
    // Put the names of the tables on the path at the correct position.
    final List<Set<String>> tableNames = new ArrayList<>();
    for (final String tableName: path) {
      tableNames.add(ImmutableSet.of(tableName));

      // Reserve a place to put tables not on the path.
      tableNames.add(new HashSet<>());
    }

    // Stores non-nullified tables that are not the superset of any nullified tables.
    // This is possible when the query is not fully nullified.
    final Set<String> restTables = new HashSet<>();

    // Put the table names of non-nullified tables at the correct position.
    final Set<String> nullifiedTables = ImmutableSet.copyOf(path);
    for (final String tableName: nullificationSetMap.keySet()) {
      // Skips if it is a nullified table (i.e., on the given path).
      if (nullifiedTables.contains(tableName)) {
        continue;
      }

      // Finds out the largest nullified table to put this non-nullified table after.
      final Set<RexNode> currentSet = nullificationSetMap.get(tableName);
      boolean inserted = false;
      for (int i = tableNames.size() - 1; i >= 0; i -= 2) {
        final String nullifiedTableName = Iterables.getOnlyElement(tableNames.get(i - 1));
        final Set<RexNode> nullifiedSet = nullificationSetMap.get(nullifiedTableName);

        // Inserts if current node is its superset.
        if (isSuperSet(currentSet, nullifiedSet)) {
          tableNames.get(i).add(tableName);
          inserted = true;
          break;
        }
      }

      // Puts the else for processing later.
      if (!inserted) {
        restTables.add(tableName);
      }
    }

    // Finds out the proper ordering for prior path.
    final List<String> priorPath = new ArrayList<>();
    List<String> subsetTableNames;
    String nextTable = Iterables.getOnlyElement(tableNames.get(0));

    while (nextTable != null) {
      // Finds all tables that are subsets of the next table.
      final Set<RexNode> nextNullificationSet = nullificationSetMap.get(nextTable);
      subsetTableNames = restTables.stream()
          .filter(table -> isSuperSet(nextNullificationSet, nullificationSetMap.get(table)))
          .collect(Collectors.toList());

      // Stops if we already reach the root node.
      if (subsetTableNames.isEmpty()) {
        break;
      }

      // Finds largest one among those subset tables.
      nextTable = subsetTableNames.stream()
          .max(Comparator.comparingInt(table -> nullificationSetMap.get(table).size()))
          .get();
      restTables.remove(nextTable);
      priorPath.add(nextTable);
    }

    // Finds out the proper ordering for prior tables.
    final List<Set<String>> priorTableNames = new ArrayList<>();
    for (int i = priorPath.size() - 1; i >= 0; i--) {
      final String table = priorPath.get(i);
      priorTableNames.add(ImmutableSet.of(table));

      // Reserve a place for other tables.
      priorTableNames.add(new HashSet<>());
    }

    // Inserts the other tables at the proper position.
    for (final String table: restTables) {
      final Set<RexNode> nullificationSet = nullificationSetMap.get(table);

      // Finds out the largest table on prior path to put this table after.
      for (int i = priorTableNames.size() - 1; i >= 0; i -= 2) {
        final String comparedTable = Iterables.getOnlyElement(priorTableNames.get(i - 1));
        if (isSuperSet(nullificationSet, nullificationSetMap.get(comparedTable))) {
          priorTableNames.get(i).add(table);
        }
      }
    }

    // Flatten the list to construct the final result.
    final List<String> result = new ArrayList<>();
    priorTableNames.forEach(result::addAll);
    tableNames.forEach(result::addAll);
    return result;
  }

  /**
   * Derives the nullification sets for all relations in the given query plan.
   *
   * @param root is the root node of the given query plan.
   * @return a mapping from the fully qualified name of each relation to its nullification sets.
   */
  public static Map<String, Set<RexNode>> deriveNullificationSets(final RelNode root) {
    final Map<String, Set<RexNode>> nullificationSetMap = new HashMap<>();
    fillNullificationSetMap(root, nullificationSetMap, 0);
    return nullificationSetMap;
  }

  /**
   * Fills the nullification set map of a given query plan.
   *
   * @param root is the root node of the query.
   * @param nullificationSetMap is the map used to store the nullification sets for each
   *                            relation in the given query plan.
   * @param offset is the offset for fields at the current node w.r.t. root node.
   * @return a list which serves as a mapping from the index of each field to the fully
   * qualified name of the table that field belongs to.
   */
  private static List<String> fillNullificationSetMap(final RelNode root,
      final Map<String, Set<RexNode>> nullificationSetMap, final int offset) {
    if (root instanceof RelSubset) {
      final RelSubset relSubset = (RelSubset) root;
      final RelNode actual = Util.first(relSubset.getBest(), relSubset.getOriginal());
      return fillNullificationSetMap(actual, nullificationSetMap, offset);
    } else if (root instanceof TableScan) {
      final TableScan tableScan = (TableScan) root;
      final String tableName = getTableFullName(tableScan.getTable());

      // Initializes the nullification set of the base relation to be an empty set.
      nullificationSetMap.put(tableName, new HashSet<>());

      // All fields should belong to the current table.
      final int numOfFields = tableScan.getRowType().getFieldCount();
      return new ArrayList<>(Collections.nCopies(numOfFields, tableName));
    }

    // Performs a postfix order traversal and aggregates the mapping.
    final List<String> allTableNames = new ArrayList<>();
    int childOffset = offset;
    for (final RelNode input: root.getInputs()) {
      allTableNames.addAll(fillNullificationSetMap(input, nullificationSetMap, childOffset));
      childOffset += input.getRowType().getFieldCount();
    }

    // Adds more to nullification sets if current node is a join operator.
    if (root instanceof Join) {
      final Join join = (Join) root;
      final RelNode left = join.getLeft();
      final RelNode right = join.getRight();
      final RexNode condition = join.getCondition();
      final boolean nullTolerant = isNullTolerant(condition);
      final Set<RexInputRef> fields = findAllFields(condition);

      // Finds out the names of tables from left & right side. Notice: the table names here
      // may contain tables from the right side of semijoin / antijoin. These tables should
      // not be in the scope of the current node.
      final Set<String> leftTableNames = RelOptUtil.findTables(left).stream()
          .map(RelOptCustomUtil::getTableFullName)
          .collect(Collectors.toSet());
      final Set<String> rightTableNames = RelOptUtil.findTables(right).stream()
          .map(RelOptCustomUtil::getTableFullName)
          .collect(Collectors.toSet());

      // The current join predicate will always be added.
      final Set<RexNode> toAdd = new HashSet<>();
      final RexNode updatedPredicate = updatePredicateOffset(condition, offset);
      toAdd.add(updatedPredicate);

      // Populates the nullification set.
      switch (join.getJoinType()) {
      case LEFT:
        // Finds out all tables from the left side that are referred by the join predicate.
        for (final RexInputRef field: fields) {
          final String tableName = allTableNames.get(field.getIndex());
          if (!nullTolerant
              && (ENABLE_NULLIFICATION_SET_FIX || leftTableNames.contains(tableName))) {
            toAdd.addAll(nullificationSetMap.get(tableName));
          }
        }

        // Updates the nullification sets for all tables from the right side.
        for (final String name: rightTableNames) {
          final Set<RexNode> current = nullificationSetMap.get(name);
          if (current == null) {
            continue;
          }
          current.addAll(toAdd);
        }
        break;
      case RIGHT:
        // Finds out all tables from the left side that are referred by the join predicate.
        for (final RexInputRef field: fields) {
          final String tableName = allTableNames.get(field.getIndex());
          if (!nullTolerant
              && (ENABLE_NULLIFICATION_SET_FIX || rightTableNames.contains(tableName))) {
            toAdd.addAll(nullificationSetMap.get(tableName));
          }
        }

        // Updates the nullification sets for all tables from the left side.
        for (final String name: leftTableNames) {
          final Set<RexNode> current = nullificationSetMap.get(name);
          if (current == null) {
            continue;
          }
          current.addAll(toAdd);
        }
        break;
      case INNER:
      case FULL:
        // Finds out all tables from the left side that are referred by the join predicate.
        for (final RexInputRef field: fields) {
          final String tableName = allTableNames.get(field.getIndex());
          if (!nullTolerant
              && (ENABLE_NULLIFICATION_SET_FIX || leftTableNames.contains(tableName))) {
            toAdd.addAll(nullificationSetMap.get(tableName));
          }
        }

        // Finds out all tables from the right side that are referred by the join predicate.
        Set<RexNode> toAdd2 = new HashSet<>();
        toAdd2.add(join.getCondition());
        for (final RexInputRef field: fields) {
          final String tableName = allTableNames.get(field.getIndex());
          if (!nullTolerant
              && (ENABLE_NULLIFICATION_SET_FIX || rightTableNames.contains(tableName))) {
            toAdd2.addAll(nullificationSetMap.get(tableName));
          }
        }

        // Updates the nullification set for both sides.
        for (final String name: rightTableNames) {
          final Set<RexNode> current = nullificationSetMap.get(name);
          if (current == null) {
            continue;
          }
          current.addAll(toAdd);
        }
        for (final String name: leftTableNames) {
          final Set<RexNode> current = nullificationSetMap.get(name);
          if (current == null) {
            continue;
          }
          current.addAll(toAdd2);
        }
        break;
      case SEMI:
      case ANTI:
        // A left semi-join / anti-join only keeps the left side of the tuple when the join
        // predicate is evaluated to true / not true. Neither side is nullified.
        for (final String name: rightTableNames) {
          nullificationSetMap.remove(name);
        }
        break;
      default:
        throw new AssertionError("Unsupported join type: " + join.getJoinType());
      }
    } else if (root instanceof Nullify) {
      final Nullify nullify = (Nullify) root;

      // Finds all tables that are nullified by the current nullification operator.
      final Set<String> nullifyTableNames = new HashSet<>();
      for (final RexNode node: nullify.getAttributes()) {
        for (final RexInputRef field: findAllFields(node)) {
          final int index = field.getIndex();
          if (index >= allTableNames.size()) {
            continue;
          }
          nullifyTableNames.add(allTableNames.get(index));
        }
      }

      // Adds the nullification predicate to the nullification sets of tables found above.
      final RexNode updatedPredicate = updatePredicateOffset(nullify.getPredicate(), offset);
      for (final String tableName: nullifyTableNames) {
        nullificationSetMap.get(tableName).add(updatedPredicate);
      }
    }

    // Modifies the output list of table names if current node is a projection.
    if (root instanceof Project) {
      final Project project = (Project) root;
      final List<String> outputTableNames = new ArrayList<>();

      // Only returns those fields which are projected.
      for (final RexNode node: project.getProjects()) {
        final RexInputRef field = (RexInputRef) node;
        outputTableNames.add(allTableNames.get(field.getIndex()));
      }
      return outputTableNames;
    } else if (root instanceof Join && !((Join) root).getJoinType().projectsRight()) {
      final int leftSize = root.getInput(0).getRowType().getFieldCount();
      return allTableNames.subList(0, leftSize);
    }
    return allTableNames;
  }

  /**
   * Finds out all tables that are nullified by nullification operators in a given
   * query plan.
   *
   * @param root is the root node of the given query plan.
   * @return a set containing the names of all nullified tables.
   */
  public static Set<String> deriveNullificationTables(RelNode root) {
    final Set<String> tableNames = new HashSet<>();
    collectNullificationTables(root, tableNames);
    return tableNames;
  }

  /**
   * Collects a set of tables whose attributes are nullified by nullification operators
   * in a given query plan.
   *
   * @param root is the root node of the given query plan.
   * @param tables is a set used to store the names of all nullified tables.
   * @return a list which serves as a mapping from the index of each field to the fully
   * qualified name of the table that field belongs to.
   */
  private static List<String> collectNullificationTables(
      final RelNode root, final Set<String> tables) {
    if (root instanceof RelSubset) {
      final RelSubset relSubset = (RelSubset) root;
      final RelNode actual = Util.first(relSubset.getBest(), relSubset.getOriginal());
      return collectNullificationTables(actual, tables);
    } else if (root instanceof TableScan) {
      final TableScan tableScan = (TableScan) root;
      final String tableName = getTableFullName(tableScan.getTable());

      // All fields should belong to the current table.
      final int numOfFields = tableScan.getRowType().getFieldCount();
      return new ArrayList<>(Collections.nCopies(numOfFields, tableName));
    }

    // Performs a postfix order traversal and aggregates the mapping.
    final List<String> allTableNames = new ArrayList<>();
    for (final RelNode input: root.getInputs()) {
      final List<String> tableNames = collectNullificationTables(input, tables);
      allTableNames.addAll(tableNames);
    }

    // Adds more tables if the current node is a nullification operator.
    if (root instanceof Nullify) {
      final Nullify nullify = (Nullify) root;

      for (final RexNode node: nullify.getAttributes()) {
        for (final RexInputRef field: findAllFields(node)) {
          final int index = field.getIndex();
          if (index >= allTableNames.size()) {
            continue;
          }

          final String tableName = allTableNames.get(index);
          tables.add(tableName);
        }
      }
    }

    // Modifies the output list of table names if current node is a projection.
    if (root instanceof Project) {
      final Project project = (Project) root;
      final List<String> outputTableNames = new ArrayList<>();

      // Only returns those fields which are projected.
      for (final RexNode node: project.getProjects()) {
        final RexInputRef field = (RexInputRef) node;
        final int index = field.getIndex();
        if (index >= allTableNames.size()) {
          continue;
        }

        outputTableNames.add(allTableNames.get(index));
      }
      return outputTableNames;
    } else if (root instanceof Join && !((Join) root).getJoinType().projectsRight()) {
      final int leftSize = root.getInput(0).getRowType().getFieldCount();
      return allTableNames.subList(0, leftSize);
    }
    return allTableNames;
  }

  /**
   * Derives the full name of a given table.
   *
   * @param table is the given table.
   * @return a fully qualified name for the given table.
   */
  private static String getTableFullName(RelOptTable table) {
    return table.getQualifiedName().toString();
  }

  /**
   * Checks whether a given predicate is referring to any attribute in a given list
   * of fields.
   *
   * @param condition is the given predicate.
   * @param fields is the given list of fields.
   * @return true if it is referring to some attribute in the list.
   */
  public static boolean isReferringTo(RexNode condition, List<RelDataTypeField> fields) {
    // Converts to RexNode format.
    Set<RexInputRef> set = fields.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toSet());

    // Checks for each field referred by the predicate.
    return isReferringTo(condition, set);
  }

  /**
   * Checks whether a given predicate is referring to any attribute in a given set
   * of fields.
   *
   * @param condition is the given predicate.
   * @param set is the given set of fields.
   * @return true if it is referring to some attribute in the list.
   */
  public static boolean isReferringTo(RexNode condition, Set<RexInputRef> set) {
    for (RexInputRef field: findAllFields(condition)) {
      if (set.contains(field)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Finds out all the fields that a given predicate is referring to.
   *
   * @param condition is the given predicate.
   * @return a set of fields the given predicate refers to.
   */
  private static Set<RexInputRef> findAllFields(final RexNode condition) {
    if (condition instanceof RexInputRef) {
      RexInputRef field = (RexInputRef) condition;
      return Collections.singleton(field);
    } else if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;

      Set<RexInputRef> fields = new HashSet<>();
      for (RexNode operand: call.getOperands()) {
        fields.addAll(findAllFields(operand));
      }
      return fields;
    } else {
      return new HashSet<>();
    }
  }

  /**
   * Updates all fields in a given predicate by adding a given offset.
   *
   * @param input is the given predicate.
   * @param offset is the given offset.
   * @return the updated predicate with offset added.
   */
  private static RexNode updatePredicateOffset(final RexNode input, final int offset) {
    if (input instanceof RexInputRef) {
      final RexInputRef field = (RexInputRef) input;
      final int newIndex = field.getIndex() + offset;
      return new RexInputRef(newIndex, field.getType());
    } else if (input instanceof RexCall) {
      final RexCall call = (RexCall) input;

      // Converts each operand in the predicate.
      ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
      call.operands.forEach(operand -> builder.add(updatePredicateOffset(operand, offset)));

      // Re-builds the predicate.
      return call.clone(call.getType(), builder.build());
    } else {
      return input;
    }
  }

  /**
   * Finds all fields that composes the primary key of any table in a given query plan.
   * Here, we make a naive assumption that the primary key of each table consists of a
   * single field and it appears first in the query.
   *
   * @param root is the root of the given query plan.
   * @return a mapping from table name to the field of that table's primary key.
   */
  public static Map<String, RelDataTypeField> findPrimaryKeys(RelNode root) {
    // Gets all fields of the current node.
    final List<RelDataTypeField> fields = root.getRowType().getFieldList();

    // Gets the table each field belongs to.
    final List<String> tableNames = findBelongingTableName(root);

    // Finds out the primary keys.
    final Map<String, RelDataTypeField> primaryKeys = new HashMap<>();
    for (int i = 0; i < tableNames.size(); i++) {
      primaryKeys.putIfAbsent(tableNames.get(i), fields.get(i));
    }
    return primaryKeys;
  }

  /**
   * Finds the name of the table each field belongs to in a given {@link RelNode}.
   *
   * @param root is the root of the given {@link RelNode}.
   * @return a list of strings representing the table names for each field.
   */
  private static List<String> findBelongingTableName(RelNode root) {
    if (root instanceof RelSubset) {
      final RelSubset relSubset = (RelSubset) root;
      final RelNode actual = Util.first(relSubset.getBest(), relSubset.getOriginal());
      return findBelongingTableName(actual);
    } else if (root instanceof TableScan) {
      final TableScan tableScan = (TableScan) root;
      final String tableName = getTableFullName(tableScan.getTable());

      // All fields should belong to the current table.
      final int numOfFields = tableScan.getRowType().getFieldCount();
      return new ArrayList<>(Collections.nCopies(numOfFields, tableName));
    } else if (root instanceof Project) {
      final Project project = (Project) root;
      final List<String> inputTableNames = findBelongingTableName(project.getInput());
      final List<String> outputTableNames = new ArrayList<>();

      // Only returns those fields which are projected.
      for (final RexNode node: project.getProjects()) {
        final RexInputRef field = (RexInputRef) node;
        outputTableNames.add(inputTableNames.get(field.getIndex()));
      }
      return outputTableNames;
    } else if (root instanceof Join) {
      final Join join = (Join) root;
      final List<String> outputTableNames = findBelongingTableName(join.getLeft());

      // Avoid adding the right side for semi join / anti join.
      if (join.getJoinType().projectsRight()) {
        outputTableNames.addAll(findBelongingTableName(join.getRight()));
      }
      return outputTableNames;
    } else {
      final List<String> tableNames = new ArrayList<>();
      for (final RelNode input: root.getInputs()) {
        tableNames.addAll(findBelongingTableName(input));
      }
      return tableNames;
    }
  }

  /**
   * Checks whether the first set is the superset of the second set.
   *
   * @param a is the first given set.
   * @param b is the second given set.
   * @param <T> is the type of items in the two sets.
   * @return true if first set is the superset of the second set.
   */
  private static <T> boolean isSuperSet(Set<T> a, Set<T> b) {
    for (T item: b) {
      if (!a.contains(item)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks whether a given predicate is null-tolerant.
   *
   * Notice: there is a potential bug when using this method to check null-tolerance.
   * The CBA approach defines null-tolerance for a predicate when any field referenced
   * is null. However, this method checks null-tolerance for the case when all fields
   * referenced by the predicate are null.
   *
   * @param predicate is the given predicate.
   * @return true if the predicate is null-tolerant; false otherwise.
   */
  public static boolean isNullTolerant(final RexNode predicate) {
    // A quick but incorrect fix for OR operands.
    if (predicate.getKind() == SqlKind.OR) {
      final RexCall or = (RexCall) predicate;
      final Set<RexNode> orOperands = ImmutableSet.copyOf(or.getOperands());

      // Unwraps if there is only one operand.
      if (orOperands.size() == 1) {
        return isNullTolerant(or.getOperands().get(0));
      }
      return true;
    }

    return valueForAllNull(predicate) == 1;
  }

  /**
   * Evaluates the value of a given predicate when every field referred by it is
   * <code>NULL</code>.
   *
   * @param predicate is the predicate to be tested.
   * @return 1 if the predicate evaluates to <code>TRUE</code>, 0 if <code>UNKNOWN</code>,
   * -1 if <code>FALSE</code>.
   */
  public static int valueForAllNull(final RexNode predicate) {
    switch (predicate.getKind()) {
    case AND:
      final RexCall and = (RexCall) predicate;
      final Set<RexNode> andOperands = ImmutableSet.copyOf(and.getOperands());

      // Iterates through each child.
      boolean isAllTrue = true;
      boolean hasAtLeastOneFalse = false;
      for (final RexNode child: andOperands) {
        final int childValue = valueForAllNull(child);
        if (childValue != 1) {
          isAllTrue = false;
        }
        if (childValue == -1) {
          hasAtLeastOneFalse = true;
        }
      }

      return isAllTrue ? 1 : (hasAtLeastOneFalse ? -1 : 0);
    case OR:
      final RexCall or = (RexCall) predicate;
      final Set<RexNode> orOperands = ImmutableSet.copyOf(or.getOperands());

      // Iterates through each child.
      boolean isAllFalse = true;
      boolean hasAtLeastOneTrue = false;
      for (final RexNode child: orOperands) {
        final int childValue = valueForAllNull(child);
        if (childValue != -1) {
          isAllFalse = false;
        }
        if (childValue == 1) {
          hasAtLeastOneTrue = true;
        }
      }

      return hasAtLeastOneTrue ? 1 : (isAllFalse ? -1 : 0);
    case NOT:
      final RexCall not = (RexCall) predicate;
      final RexNode child = not.getOperands().get(0);
      return -valueForAllNull(child);
    case LITERAL:
      final RexLiteral literal = (RexLiteral) predicate;
      return literal.isAlwaysTrue() ? 1 : (literal.isNull() ? 0 : -1);
    case IS_NOT_TRUE:
    case IS_NOT_FALSE:
    case IS_NULL:
    case IS_UNKNOWN:
      return 1;
    case IS_TRUE:
    case IS_FALSE:
    case IS_NOT_NULL:
      return -1;
    default:
      return 0;
    }
  }

  /**
   * Converts a given query plan to its corresponding SQL query string. This
   * method will convert {@link RelSubset} by default.
   *
   * @param root is the given query plan.
   * @return the SQL query string.
   */
  public static String toQueryString(final RelNode root) {
    return toQueryString(root, true);
  }

  /**
   * Converts a given query plan to its corresponding SQL query string.
   *
   * @param root is the given query plan.
   * @param allowSubsetConvert is a flag to control whether we can convert
   *                           {@link RelSubset} to SQL query.
   * @return the SQL query string.
   */
  public static String toQueryString(RelNode root, final boolean allowSubsetConvert) {
    return toQueryString(root, allowSubsetConvert, defaultPreferJDBC);
  }

  /**
   * Converts a given query plan to its corresponding SQL query string.
   *
   * @param root is the given query plan.
   * @param allowSubsetConvert is a flag to control whether we can convert
   *                           {@link RelSubset} to SQL query.
   * @param useJDBC indicates whether a {@link JdbcImplementor} should be used.
   * @return the SQL query string.
   */
  public static String toQueryString(RelNode root, final boolean allowSubsetConvert,
      final boolean useJDBC) {
    final RelToSqlConverter converter = useJDBC
        ? new JdbcImplementor(OUTPUT_DIALECT, allowSubsetConvert)
        : new RelToSqlConverter(OUTPUT_DIALECT, allowSubsetConvert);
    final SqlNode transformedSqlNode = converter.visitChild(0, root).asStatement();
    return transformedSqlNode.toSqlString(OUTPUT_DIALECT).getSql();
  }

  /**
   * Registers a pair of {@link RelNode} to identify that they go through a
   * transformation via a {@link RelOptRule}. Duplicate pairs will not be
   * registered twice.
   *
   * @param before is the query plan before transformation.
   * @param after is the query plan after transformation.
   * @param description is the description for this change.
   */
  public static void registerTransformationPair(RelNode before, RelNode after, String description) {
    if (!ENABLE_TRANSFORMATION_RECORDING) {
      return;
    }

    final String beforeQuery = toQueryString(before);
    final String afterQuery = toQueryString(after);
    transformationPairs.put(Pair.of(beforeQuery, afterQuery), description);
  }

  /**
   * Outputs the result using a given writer.
   *
   * @param writer is the given writer.
   * @param origin is the original {@link SqlNode} before any transformation happens.
   */
  public static void outputTransformationPairs(Writer writer, SqlNode origin) throws Exception {
    for (final Map.Entry<Pair<String, String>, String> entry: transformationPairs.entrySet()) {
      writer.write(PAIR_DELIMITER);
      writer.write(origin.toSqlString(OUTPUT_DIALECT).getSql() + "\n");
      writer.write(INTERNAL_DELIMITER);
      writer.write(entry.getKey().left + "\n");
      writer.write(INTERNAL_DELIMITER);
      writer.write(entry.getKey().right + "\n");
      writer.write(INTERNAL_DELIMITER);
      writer.write(entry.getValue() + "\n");
      writer.write(PAIR_DELIMITER);
    }
    transformationPairs.clear();
  }

  /**
   * Outputs a pair of queries.
   *
   * @param writer is the given writer.
   * @param first is the first query.
   * @param second is the second query.
   * @throws Exception when cannot output to the writer.
   */
  public static void outputTransformationPair(final Writer writer, final int count,
      final String first, final String second) throws Exception {
    writer.write(PAIR_DELIMITER);
    writer.write("Query count: #" + count + "\n");
    writer.write(INTERNAL_DELIMITER);
    writer.write(first + "\n");
    writer.write(INTERNAL_DELIMITER);
    writer.write(second + "\n");
    writer.write(PAIR_DELIMITER);
  }

  public static void outputStatistics() {
    System.out.println("\nStatistics for # of passes required to implement best-match:");
    System.out.print(INTERNAL_DELIMITER);
    System.out.println("# of passes | # of queries");
    for (final int numPasses: STATISTICS.keySet()) {
      System.out.printf("%11d | %d\n", numPasses, STATISTICS.get(numPasses));
    }
    System.out.print(INTERNAL_DELIMITER);
  }
}

// End RelOptCustomUtil.java
