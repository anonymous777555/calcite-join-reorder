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
package org.apache.calcite.util.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AcyclicDirectedGraph is an abstract data type to represent DAG. It ensures
 * no cycle in the graph and provides additional methods.
 */
public class AcyclicDirectedGraph extends DefaultDirectedGraph<String, AcyclicDefaultEdge> {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new directed acyclic graph.
   *
   * @param edgeFactory is the edge factory.
   */
  public AcyclicDirectedGraph(final EdgeFactory<String, AcyclicDefaultEdge> edgeFactory) {
    super(edgeFactory);
  }

  public static AcyclicDirectedGraph create() {
    return new AcyclicDirectedGraph(AcyclicDefaultEdge.factory());
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Computes the minimum (vertex-disjoint) path cover for this DAG. We reduce it to
   * the maximum matching problem on a derived {@link BipartiteDirectedGraph}.
   *
   * @return a set containing all paths in the derived minimum cover.
   */
  public Collection<List<String>> minimumPathCover() {
    // Creates an associated bipartite graph.
    final BipartiteDirectedGraph<String, AcyclicDefaultEdge> graph = BipartiteDirectedGraph.create(
        AcyclicDefaultEdge.factory());
    for (final String vertex: vertexMap.keySet()) {
      graph.addVertex(vertex + "_1", true);
      graph.addVertex(vertex + "_2", false);
    }
    for (final AcyclicDefaultEdge edge: edges) {
      graph.addEdge(edge.source + "_1", edge.target + "_2");
    }

    // Computes the maximum matching for the constructed bipartite graph.
    final Collection<AcyclicDefaultEdge> matching = graph.maximumMatching();

    // Converts the maximum matching into minimum path cover by applying a variant of
    // the union-find algorithm.
    final Map<String, Integer> valueToGroup = new HashMap<>();
    final Map<Integer, List<String>> groupToValues = new HashMap<>();

    // Initializes each vertex to be within an individual group.
    int count = 0;
    for (final String vertex: vertexMap.keySet()) {
      final List<String> values = new ArrayList<>();
      values.add(vertex);
      groupToValues.put(count, values);

      valueToGroup.put(vertex, count);
      count++;
    }

    // Unions two groups whenever they are connected.
    for (final AcyclicDefaultEdge edge: matching) {
      final String start = edge.source.substring(0, edge.source.length() - 2);
      final String end = edge.target.substring(0, edge.target.length() - 2);

      // Retrieves the groups of the current two vertices.
      final int startGroupID = valueToGroup.get(start);
      final int endGroupID = valueToGroup.get(end);
      final List<String> startGroup = groupToValues.get(startGroupID);
      final List<String> endGroup = groupToValues.get(endGroupID);

      // Merges the two groups (this step is actually union-find algorithm).
      for (String node: endGroup) {
        startGroup.add(node);
        valueToGroup.put(node, startGroupID);
      }
      groupToValues.remove(endGroupID);
    }

    // Returns all derived paths.
    return groupToValues.values();
  }
}

// End AcyclicDirectedGraph.java
