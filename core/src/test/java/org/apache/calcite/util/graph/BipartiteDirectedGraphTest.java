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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link BipartiteDirectedGraph}.
 */
public class BipartiteDirectedGraphTest {
  @Rule public final ExpectedException exception = ExpectedException.none();

  public BipartiteDirectedGraphTest() {
  }

  @Test public void testAddVertex() {
    BipartiteDirectedGraph<String, DefaultEdge> graph = BipartiteDirectedGraph.create();

    // Disallow adding vertices without side.
    assertFalse(graph.addVertex(""));
    assertFalse(graph.addVertex("A"));

    // Normal cases.
    assertTrue(graph.addVertex("A", true));
    assertFalse(graph.addVertex("A", true));
    assertFalse(graph.addVertex("A", false));
    assertTrue(graph.addVertex("B", true));
    assertTrue(graph.addVertex("C", false));
  }

  @Test public void testAddEdge() {
    BipartiteDirectedGraph<String, DefaultEdge> graph = BipartiteDirectedGraph.create();
    graph.addVertex("A", true);
    graph.addVertex("B", true);
    graph.addVertex("C", false);

    assertNotNull(graph.addEdge("A", "C"));
    assertNull(graph.addEdge("A", "C"));

    // Disallow adding edges within the same side.
    exception.expect(IllegalArgumentException.class);
    graph.addEdge("A", "B");
  }

  @Test public void testMaximumMatching() {
    BipartiteDirectedGraph<String, DefaultEdge> graph = BipartiteDirectedGraph.create();
    graph.addVertex("A", true);
    graph.addVertex("B", true);
    graph.addVertex("C", true);
    graph.addVertex("D", false);
    graph.addVertex("E", false);
    graph.addVertex("F", false);

    Collection<DefaultEdge> matching = graph.maximumMatching();
    assertEquals(0, matching.size());

    graph.addEdge("A", "D");
    matching = graph.maximumMatching();
    assertEquals(1, matching.size());

    graph.addEdge("A", "E");
    matching = graph.maximumMatching();
    assertEquals(1, matching.size());

    graph.addEdge("B", "E");
    matching = graph.maximumMatching();
    assertEquals(2, matching.size());

    graph.addEdge("C", "E");
    matching = graph.maximumMatching();
    assertEquals(2, matching.size());

    graph.addEdge("A", "F");
    matching = graph.maximumMatching();
    assertEquals(2, matching.size());

    graph.addEdge("B", "F");
    matching = graph.maximumMatching();
    assertEquals(3, matching.size());

    graph.addEdge("C", "F");
    matching = graph.maximumMatching();
    assertEquals(3, matching.size());
  }

  @Test public void testComplexMaximumMatching() {
    BipartiteDirectedGraph<String, DefaultEdge> graph = BipartiteDirectedGraph.create();
    graph.addVertex("a1", true);
    graph.addVertex("a2", true);
    graph.addVertex("a3", true);
    graph.addVertex("a4", true);
    graph.addVertex("a5", true);
    graph.addVertex("a6", true);
    graph.addVertex("b1", false);
    graph.addVertex("b2", false);
    graph.addVertex("b3", false);
    graph.addVertex("b4", false);
    graph.addVertex("b5", false);
    graph.addVertex("b6", false);

    Collection<DefaultEdge> matching = graph.maximumMatching();
    assertEquals(0, matching.size());

    graph.addEdge("a1", "b2");
    graph.addEdge("a1", "b3");
    graph.addEdge("a3", "b1");
    graph.addEdge("a3", "b4");
    graph.addEdge("a4", "b3");
    graph.addEdge("a5", "b3");
    graph.addEdge("a5", "b4");
    graph.addEdge("a6", "b6");

    matching = graph.maximumMatching();
    assertEquals(5, matching.size());
  }
}

// End BipartiteDirectedGraphTest.java
