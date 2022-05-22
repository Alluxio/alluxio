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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests {@link TrieNode}.
 */
public class TrieNodeTest {
  @Test
  public void insertAndCheckChildren() {
    TrieNode<Long> root = new TrieNode<>();

    List<Long> ids1 = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L));
    List<Long> ids2 = new ArrayList<>(Arrays.asList(1L, 2L));
    List<Long> ids3 = new ArrayList<>(Arrays.asList(1L, 2L, 5L));
    List<Long> ids4 = new ArrayList<>(Arrays.asList(1L, 2L, 3L));

    root.insert(ids1, true);
    root.insert(ids2, true);
    root.insert(ids4, true);
    TrieNode<Long> node1 = root.insert(ids3, true);
    TrieNode<Long> node2 = root.lowestMatchedTrieNode(ids2, n -> true, true);
    TrieNode<Long> node3 = root.lowestMatchedTrieNode(ids4, n -> true, true);
    Assert.assertNull(node1.child(6L, n -> true));
    Assert.assertNull(node2.child(4L, n -> true));
    Assert.assertNotNull(node3);
  }

  @Test
  public void removeAndCheck() {
    TrieNode<Long> root = new TrieNode<>();
    List<Long> ids0 = new ArrayList<>(Arrays.asList(1L));
    List<Long> ids1 = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L));
    List<Long> ids2 = new ArrayList<>(Arrays.asList(1L, 2L, 5L));
    List<Long> idsn = new ArrayList<>(Arrays.asList(1L, 2L));
    List<Long> idsToRemove = new ArrayList<>(Arrays.asList(1L, 20L));
    root.insert(ids0, true);
    root.insert(ids1, true);
    root.insert(ids2, true);
    root.insert(idsn, true);
    root.insert(idsToRemove, true);
    List<Long> ids3 = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
    List<Long> ids4 = new ArrayList<>(Arrays.asList(1L, 2L, 5L));
    List<Long> ids5 = new ArrayList<>(Arrays.asList(1L, 10L));
    TrieNode<Long> node1 = root.remove(ids5, n -> true);
    Assert.assertNull(node1);
    TrieNode<Long> node2 = root.remove(ids3, n -> true);
    Assert.assertNotNull(node2);
    TrieNode<Long> node3 = root.remove(ids4, n -> true);
    Assert.assertNotNull(node3);
    TrieNode<Long> node4 = root.remove(idsToRemove,
        TrieNode::isTerminal);
    Assert.assertNotNull(node4);
    List<Long> ids6 = new ArrayList<>(Arrays.asList(1L, 23L, 24L));
    root.insert(ids6, true);
    Assert.assertNotNull(root.remove(ids6, n -> true));
  }

  @Test
  public void matchInodes() {
    TrieNode<Long> root = new TrieNode<>();
    List<Long> ids1 = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 5L));
    List<Long> ids2 = new ArrayList<>(Arrays.asList(1L, 2L, 6L, 7L));
    List<Long> ids3 = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
    List<Long> ids4 = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 10L));
    List<Long> ids5 = new ArrayList<>(Arrays.asList(1L, 2L));
    List<Long> ids6 = new ArrayList<>(Arrays.asList(1L, 3L));
    List<Long> ids7 = new ArrayList<>(Arrays.asList(12L, 13L));

    TrieNode<Long> n1 = root.insert(ids1, true);
    root.insert(ids2, true);
    root.insert(ids3, false);
    root.insert(ids4, true);
    root.insert(ids5, false);
    TrieNode<Long> n2 = n1.insert(ids7, true);

    Assert.assertEquals(ids1, n1.list());
    ids1.addAll(ids7);
    Assert.assertEquals(ids1, n2.list());
    TrieNode<Long> node1 = root.lowestMatchedTrieNode(
        ids3, n -> n.isMountPoint(), true
    );
    TrieNode<Long> node2 = root.lowestMatchedTrieNode(
        ids4, n -> !n.isMountPoint(), false
    );
    TrieNode<Long> node3 = root.lowestMatchedTrieNode(
        ids4, n -> !n.isMountPoint(), true
    );
    TrieNode<Long> node4 = node2.lowestMatchedTrieNode(new ArrayList<>(), n -> true,
        true);
    TrieNode<Long> node5 = root.lowestMatchedTrieNode(
        ids6, n -> true, true
    );
    TrieNode<Long> node6 = root.lowestMatchedTrieNode(
        ids5, n -> true, true
    );
    Assert.assertNull(node1);
    Assert.assertNotNull(node2);
    Assert.assertNull(node5);
    Assert.assertNotNull(node6.child(3L, TrieNode::isTerminal));
    Assert.assertEquals(node2, node4);
    Assert.assertNull(node3);
  }

  @Test
  public void allChildren() {
    List<Long> ids1 = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L));
    List<Long> ids2 = new ArrayList<>(Arrays.asList(1L, 2L, 5L));
    List<Long> ids3 = new ArrayList<>(Arrays.asList(1L, 2L, 6L));
    List<Long> ids4 = new ArrayList<>(Arrays.asList(1L, 2L));

    TrieNode<Long> root = new TrieNode<>();
    TrieNode<Long> n1 = root.insert(ids1, true);
    root.insert(ids2, true);
    root.insert(ids3, true);
    TrieNode<Long> n2 = root.insert(ids4, true);

    List<TrieNode<Long>> n1ChildrenTrieNode =
        n1.allChildrenTrieNode(TrieNode::isTerminal, false);
    List<TrieNode<Long>> n2ChildrenTrieNode =
        n2.allChildrenTrieNode(TrieNode::isTerminal, false);

    Assert.assertEquals(new ArrayList<List<Long>>(),
        n1ChildrenTrieNode.stream()
            .map(TrieNode::list).collect(Collectors.toList()));
    Assert.assertEquals(new ArrayList<List<Long>>() {
      {
        add(ids2);
        add(ids3);
        add(ids1);
      }
    }, n2ChildrenTrieNode.stream().map(TrieNode::list).collect(Collectors.toList()));
    Assert.assertNull(n2.child(5L, n -> !n.isTerminal()));
  }
}
