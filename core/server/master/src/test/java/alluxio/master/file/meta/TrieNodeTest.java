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

package alluxio.master.file.meta;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * Tests {@link TrieNode}.
 */
public class TrieNodeTest extends BaseInodeLockingTest {
  @Test
  public void testInsert() {
    TrieNode<Integer> root = new TrieNode<>();
    root.insert(Arrays.asList(1, 2, 3));
    root.insert(Arrays.asList(1, 4, 5));
    root.insert(Arrays.asList(1, 6));
    root.insert(Arrays.asList(1, 4, 9));

    TrieNode<Integer> node1 = root.lowestMatchedTrieNode(Collections.singletonList(1),
        false, true);

    Assert.assertEquals(Sets.newHashSet(2, 4, 6), node1.childrenKeys());
    Assert.assertEquals(Sets.newHashSet(5, 9), node1.child(4).childrenKeys());
    Assert.assertNull(root.child(10));
  }

  @Test
  public void testLowestMatchedTrieNode() {
    TrieNode<Integer> root = new TrieNode<>();

    root.insert(Arrays.asList(1, 2, 3));
    root.insert(Arrays.asList(1, 4, 5));
    root.insert(Arrays.asList(1, 6));
    root.insert(Arrays.asList(1, 4, 9));

    TrieNode<Integer> node1 = root.lowestMatchedTrieNode(Collections.singletonList(1),
        false, false);
    Assert.assertEquals(Sets.newHashSet(2, 4, 6), node1.childrenKeys());
    Assert.assertEquals(node1, node1.lowestMatchedTrieNode(new ArrayList<>(), false, false));
    Assert.assertNull(node1.lowestMatchedTrieNode(new ArrayList<>(), true, false));
    Assert.assertNull(node1.lowestMatchedTrieNode(Arrays.asList(3, 5), true, true));
    Assert.assertNull(node1.lowestMatchedTrieNode(Arrays.asList(3, 5), true, false));
    Assert.assertNotNull(node1.lowestMatchedTrieNode(Arrays.asList(3, 5), false, false));
  }

  @Test
  public void testRemove() {
    TrieNode<Integer> root = new TrieNode<>();

    root.insert(Arrays.asList(1, 2, 3));
    root.insert(Arrays.asList(1, 4, 5));
    root.insert(Arrays.asList(1, 6));
    root.insert(Arrays.asList(1, 4, 9));

    TrieNode<Integer> node1 = root.lowestMatchedTrieNode(Collections.singletonList(1), false,
        true);
    // remove a path that is not existed will return null
    Assert.assertNull(root.remove(Arrays.asList(1, 7, 9)));
    // remove a non-terminal path will return null
    Assert.assertNull(root.remove(Arrays.asList(1, 4)));
    // remove a terminal path successfully
    Assert.assertNotNull(root.remove(Arrays.asList(1, 4, 9)));
    // node '4' has child after the remove, so it is still the child of node '1'
    Assert.assertNotNull(node1.child(4));
    Assert.assertNotNull(root.remove(Arrays.asList(1, 4, 5)));
    // node '4' has no child after the remove, so it will be removed by the above remove call
    Assert.assertNull(node1.child(4));
  }

  @Test
  public void testDescendents() {
    TrieNode<Integer> root = new TrieNode<>();

    root.insert(Arrays.asList(1, 2, 3));
    root.insert(Arrays.asList(1, 4));

    TrieNode<Integer> node1 = root.lowestMatchedTrieNode(Collections.singletonList(1), false, true);
    TrieNode<Integer> node2 = node1.lowestMatchedTrieNode(Collections.singletonList(2), false,
        true);
    TrieNode<Integer> node3 = node2.lowestMatchedTrieNode(Collections.singletonList(3), false,
        true);
    TrieNode<Integer> node4 = node1.lowestMatchedTrieNode(Collections.singletonList(4), false,
        true);

    Assert.assertEquals(Arrays.asList(node1, node2, node4, node3), root.descendants(false,
        false, false));
    Assert.assertEquals(Arrays.asList(root, node1, node2, node4, node3), root.descendants(false,
        true, false));
    Assert.assertEquals(Arrays.asList(node4, node3), root.descendants(true, true, false));
    Assert.assertTrue(node2.hasNestedTerminalTrieNodes(false));
    Assert.assertFalse(node3.hasNestedTerminalTrieNodes(false));
  }
}
