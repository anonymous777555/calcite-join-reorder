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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * BipartiteDirectedGraph is a directed graph whose vertices can be divided into
 * two disjoint sets, L and R. Each directed edge in the graph must start from a
 * vertex in L and end at a vertex in R.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class BipartiteDirectedGraph<V, E extends DefaultEdge>
    extends DefaultDirectedGraph<V, E> {
  //~ Instance fields --------------------------------------------------------

  final Set<V> left = new LinkedHashSet<>();
  final Set<V> right = new LinkedHashSet<>();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new bipartite graph.
   *
   * @param edgeFactory is the edge factory.
   */
  public BipartiteDirectedGraph(final EdgeFactory<V, E> edgeFactory) {
    super(edgeFactory);
  }

  public static <V> BipartiteDirectedGraph<V, DefaultEdge> create() {
    return create(DefaultEdge.factory());
  }

  public static <V, E extends DefaultEdge> BipartiteDirectedGraph<V, E> create(
      EdgeFactory<V, E> edgeFactory) {
    return new BipartiteDirectedGraph<>(edgeFactory);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Adds a new vertex into the current bipartite graph.
   *
   * @param vertex is the newly added vertex.
   * @param fromLeft indicates whether the new vertex should be in set L.
   * @return true if the vertex is added successfully.
   */
  public boolean addVertex(final V vertex, final boolean fromLeft) {
    if (!super.addVertex(vertex)) {
      return false;
    }

    // Adds this new vertex into the correct set, L or R.
    if (fromLeft) {
      left.add(vertex);
    } else {
      right.add(vertex);
    }
    return true;
  }

  /**
   * Vertices in a bipartite graph has to be from L or R. It is compulsory to specify
   * in which set this new vertex should be.
   *
   * @param vertex is the newly added vertex.
   * @return false since it is always preferred to specify L or R.
   */
  @Override public boolean addVertex(final V vertex) {
    return false;
  }

  /**
   * Adds a new directed edge (from L to R) into this bipartite graph.
   *
   * @param vertex is the starting vertex (in L) of this edge.
   * @param targetVertex is the ending vertex (in R) of this edge.
   * @return the new edge added, otherwise <code>NULL</code> if the edge already exists.
   * @throws IllegalArgumentException if either vertex is not already in this graph
   * or the new edge is not from L to R.
   */
  @Override public E addEdge(final V vertex, final V targetVertex) {
    if (!left.contains(vertex)) {
      throw new IllegalArgumentException("start vertex " + vertex + " is not from left.");
    } else if (!right.contains(targetVertex)) {
      throw new IllegalArgumentException("end vertex " + targetVertex + " is not from right.");
    }
    return super.addEdge(vertex, targetVertex);
  }

  /**
   * Removes a set of vertices (and their related edges) from this graph.
   *
   * @param collection is a collection containing all vertices to be removed.
   */
  @Override public void removeAllVertices(final Collection<V> collection) {
    // Removes from both sets, L and R.
    left.removeAll(collection);
    right.removeAll(collection);

    // Removes from super-class.
    super.removeAllVertices(collection);
  }

  /**
   * Computes the maximum cardinality matching for this bipartite graph. We reduce it to a
   * maximum network flow problem, which can then be solved by Ford-Fulkerson algorithm. It
   * is interesting to note that we implement a simplified version of Fold-Fulkerson as this
   * is a non-weighted bipartite graph.
   *
   * @return a collection containing all edges in the maximum matching.
   */
  public Collection<E> maximumMatching() {
    // A mapping to record down the matching to the right side.
    final Map<V, E> matchForRight = new HashMap<>();

    // Iterates through the left side.
    for (final V vertex: left) {
      singleMatch(vertex, new HashSet<>(), matchForRight);
    }
    return matchForRight.values();
  }

  /**
   * A DFS-based recursive traversal trying to find a matching for a left vertex.
   *
   * @param vertex is the vertex from left side.
   * @param seen is a set containing all right vertices that have been matched.
   * @param matchForRight contains the edges mapping to right side vertices.
   * @return true if we found a matching for the left vertex.
   */
  private boolean singleMatch(final V vertex, final Set<V> seen, final Map<V, E> matchForRight) {
    // Iterates through the right side.
    for (final V rightVertex: right) {
      final E edge = getEdge(vertex, rightVertex);
      if (edge != null && !seen.contains(rightVertex)) {
        // Marks the right vertex as seen.
        seen.add(rightVertex);

        // Checks whether this right vertex has not been matched or the original matching has an
        // alternating path.
        final E originalMatch = matchForRight.get(rightVertex);
        if (originalMatch == null || singleMatch((V) originalMatch.source, seen, matchForRight)) {
          matchForRight.put(rightVertex, edge);
          return true;
        }
      }
    }

    // Returns false since we cannot find any matching.
    return false;
  }
}

// End BipartiteDirectedGraph.java
