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

    TrieNode<Integer> node1 = root.lowestMatchedTrieNode(Arrays.asList(1), false, true);

    Assert.assertEquals(Sets.newHashSet(2, 4, 6), node1.childrenKeys());
    Assert.assertEquals(Sets.newHashSet(5, 9), node1.child(4).childrenKeys());
    Assert.assertNull(root.child(10));
  }

  @Test
  public void testMountTableTriePredicate() {
    TrieNode<InodeView> root = new TrieNode<>();
    // insert /mnt/foo/sub/f1, f1 is an emptyNode
    EmptyInode fileF1 = new EmptyInode("f1");
    EmptyInode dirBaz = new EmptyInode("baz");
    root.insert(Arrays.asList(mDirMnt, mDirFoo, mDirSub, fileF1));

    TrieNode<InodeView> nodeSub = root.lowestMatchedTrieNode(Arrays.asList(mDirMnt, mDirFoo,
        mDirSub), false, true);
    Assert.assertEquals(nodeSub.childrenKeys(), Sets.newHashSet(fileF1));
    // insert /mnt/foo/sub/f1 again, there is no emptyNode
    root.insert(Arrays.asList(mDirMnt, mDirFoo, mDirSub, mFileF1),
        MountTable.MountTableTrieOperator::checkAndSubstitute);

    Assert.assertEquals(Sets.newHashSet(mFileF1), nodeSub.childrenKeys());
    // insert /mnt/bar/baz, baz is an emptyInode
    root.insert(Arrays.asList(mDirMnt, mDirBar, dirBaz),
        MountTable.MountTableTrieOperator::checkAndSubstitute);
    TrieNode<InodeView> nodeBar = root.lowestMatchedTrieNode(Arrays.asList(mDirMnt, mDirBar),
        false, false);
    Assert.assertEquals(Sets.newHashSet(dirBaz), nodeBar.childrenKeys());
    TrieNode<InodeView> nodeBaz = root.lowestMatchedTrieNode(Arrays.asList(mDirMnt, mDirBar,
        mDirBaz, mFileBay), true,
        MountTable.MountTableTrieOperator::checkAndSubstitute, false);
    Assert.assertEquals(Sets.newHashSet(mDirBaz), nodeBar.childrenKeys());
  }

  @Test
  public void testLowestMatchedTrieNode() {
    TrieNode<Integer> root = new TrieNode<>();

    root.insert(Arrays.asList(1, 2, 3));
    root.insert(Arrays.asList(1, 4, 5));
    root.insert(Arrays.asList(1, 6));
    root.insert(Arrays.asList(1, 4, 9));

    TrieNode<Integer> node1 = root.lowestMatchedTrieNode(Arrays.asList(1), false, false);
    Assert.assertEquals(Sets.newHashSet(2, 4, 6), node1.childrenKeys());
    Assert.assertEquals(node1, node1.lowestMatchedTrieNode(new ArrayList<>(), false, false));
    Assert.assertNull(node1.lowestMatchedTrieNode(new ArrayList<>(), true, false));
    Assert.assertNull(node1.lowestMatchedTrieNode(Arrays.asList(3, 5), true, true));
    Assert.assertNull(node1.lowestMatchedTrieNode(Arrays.asList(3, 5), true, false));
    Assert.assertNotNull(node1.lowestMatchedTrieNode(Arrays.asList(3, 5), false, false));
  }
}
