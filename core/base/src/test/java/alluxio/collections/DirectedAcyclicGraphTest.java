/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests the {@link DirectedAcyclicGraph} class.
 */
public final class DirectedAcyclicGraphTest {
  private DirectedAcyclicGraph<Integer> mGraph;

  /**
   * Initialize.
   */
  @Before
  public void before() {
    mGraph = new DirectedAcyclicGraph<>();
  }

  /**
   * Tests some simple graphs.
   */
  @Test
  public void simpleGraph() {
    // Empty graph.
    assertTrue(mGraph.getRoots().isEmpty());
    assertTrue(mGraph.sortTopologically(new HashSet<Integer>()).isEmpty());

    // One node.
    mGraph.add(1, new ArrayList<Integer>());
    assertEquals(1, mGraph.getRoots().size());
    assertEquals((Integer) (1), mGraph.getRoots().get(0));
    mGraph.deleteLeaf(1);
    assertTrue(mGraph.getRoots().isEmpty());
  }

  /**
   * More complicated graph to test topological sort. Other functionalities such as add
   * are tested indirectly also.
   */
  @Test
  public void topologicalSort() {
    // Construct a graph.
    // 1->2, 3->2, 4->2, 5->1, 6
    List<Integer> parents = new ArrayList<>();

    mGraph.add(5, parents);
    parents.add(5);
    mGraph.add(1, parents);
    parents.clear();
    mGraph.add(3, parents);
    mGraph.add(4, parents);
    parents.add(1);
    parents.add(3);
    parents.add(4);
    mGraph.add(2, parents);
    parents.clear();
    mGraph.add(6, parents);

    // Sort the whole graph.
    Set<Integer> toSort = new HashSet<>();
    for (int i = 1; i <= 6; i++) {
      toSort.add(i);
    }

    List<Integer> result;
    Set<Integer> seen = new HashSet<>();

    result = mGraph.sortTopologically(toSort);
    for (Integer i : result) {
      if (i == 1) {
        assertTrue(seen.contains(5));
      } else if (i == 2) {
        assertTrue(seen.contains(1));
        assertTrue(seen.contains(3));
        assertTrue(seen.contains(4));
      }
      seen.add(i);
    }
    assertEquals(6, seen.size());

    // Sort part of the graph.
    toSort.clear();
    toSort.add(1);
    toSort.add(2);
    seen.clear();

    result = mGraph.sortTopologically(toSort);
    for (Integer i : result) {
      if (i == 2) {
        assertTrue(seen.contains(1));
      }
      seen.add(i);
    }
    assertEquals(2, seen.size());
  }
}

