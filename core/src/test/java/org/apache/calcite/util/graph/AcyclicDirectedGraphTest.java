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

import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link AcyclicDirectedGraph}.
 */
public class AcyclicDirectedGraphTest {
  @Test public void testCreate() {
    AcyclicDirectedGraph graph = AcyclicDirectedGraph.create();

    assertTrue(graph.addVertex("A"));
    assertFalse(graph.addVertex("A"));
    assertTrue(graph.addVertex("B"));

    assertNotNull(graph.addEdge("A", "B"));
    assertNull(graph.addEdge("A", "B"));
  }

  @Test public void testMinimumPathCover() {
    AcyclicDirectedGraph graph = AcyclicDirectedGraph.create();
    graph.addVertex("A");
    graph.addVertex("B");
    graph.addVertex("C");

    Collection<List<String>> minimumCover = graph.minimumPathCover();
    assertEquals(3, minimumCover.size());

    graph.addEdge("A", "B");
    minimumCover = graph.minimumPathCover();
    assertEquals(2, minimumCover.size());

    graph.addEdge("A", "C");
    minimumCover = graph.minimumPathCover();
    assertEquals(2, minimumCover.size());

    graph.addEdge("B", "C");
    minimumCover = graph.minimumPathCover();
    assertEquals(1, minimumCover.size());
  }
}

// End AcyclicDirectedGraphTest.java
