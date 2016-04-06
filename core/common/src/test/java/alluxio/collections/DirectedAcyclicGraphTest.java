/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.collections;

import org.junit.Assert;
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
  public void simpleGraphTest() {
    // Empty graph.
    Assert.assertTrue(mGraph.getRoots().isEmpty());
    Assert.assertTrue(mGraph.sortTopologically(new HashSet<Integer>()).isEmpty());

    // One node.
    mGraph.add((Integer) (1), new ArrayList<Integer>());
    Assert.assertEquals(1, mGraph.getRoots().size());
    Assert.assertEquals((Integer) (1), mGraph.getRoots().get(0));
    mGraph.deleteLeaf((Integer) (1));
    Assert.assertTrue(mGraph.getRoots().isEmpty());
  }

  /**
   * More complicated graph to test topological sort. Other functionalities such as add
   * are tested indirectly also.
   */
  @Test
  public void topologicalSortTest() {
    // Construct a graph.
    // 1->2, 3->2, 4->2, 5->1, 6
    List<Integer> parents = new ArrayList<Integer>();

    mGraph.add((Integer) (5), parents);
    parents.add((Integer) (5));
    mGraph.add((Integer) (1), parents);
    parents.clear();
    mGraph.add((Integer) (3), parents);
    mGraph.add((Integer) (4), parents);
    parents.add((Integer) (1));
    parents.add((Integer) (3));
    parents.add((Integer) (4));
    mGraph.add((Integer) (2), parents);
    parents.clear();
    mGraph.add((Integer) (6), parents);

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
        Assert.assertTrue(seen.contains((Integer) (5)));
      } else if (i == 2) {
        Assert.assertTrue(seen.contains((Integer) (1)));
        Assert.assertTrue(seen.contains((Integer) (3)));
        Assert.assertTrue(seen.contains((Integer) (4)));
      }
      seen.add(i);
    }
    Assert.assertEquals(6, seen.size());

    // Sort part of the graph.
    toSort.clear();
    toSort.add((Integer) (1));
    toSort.add((Integer) (2));
    seen.clear();

    result = mGraph.sortTopologically(toSort);
    for (Integer i : result) {
      if (i == 2) {
        Assert.assertTrue(seen.contains((Integer) (1)));
      }
      seen.add(i);
    }
    Assert.assertEquals(2, seen.size());
  }
}

