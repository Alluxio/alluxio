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

import java.util.Arrays;

/**
 * Tests {@link InodeTrieNode}.
 */
public class InodeTrieNodeTest extends BaseInodeLockingTest {
  @Test
  public void testMountTableTriePredicate() {
    InodeTrieNode root = new InodeTrieNode();
    // insert /mnt/foo/sub/f1, f1 is an emptyNode
    EmptyInode fileF1 = new EmptyInode("f1");
    EmptyInode dirBaz = new EmptyInode("baz");
    EmptyInode dirOkc = new EmptyInode("okc");
    root.insert(Arrays.asList(mDirMnt, mDirFoo, mDirSub, fileF1));

    TrieNode<InodeView> nodeSub = root.lowestMatchedTrieNode(Arrays.asList(mDirMnt, mDirFoo,
        mDirSub), false,  true);
    Assert.assertEquals(nodeSub.childrenKeys(), Sets.newHashSet(fileF1));
    // insert /mnt/foo/sub/f1 again, there is no emptyNode
    root.insert(Arrays.asList(mDirMnt, mDirFoo, mDirSub, mFileF1));
    Assert.assertEquals(Sets.newHashSet(mFileF1), nodeSub.childrenKeys());
    // insert /mnt/bar/baz, baz is an emptyInode
    root.insert(Arrays.asList(mDirMnt, mDirBar, dirBaz));
    // insert /mnt/bar/baz/okc, okc is an emptyInode
    root.insert(Arrays.asList(mDirMnt, mDirBar, new EmptyInode("baz"), dirOkc));

    TrieNode<InodeView> nodeBar = root.lowestMatchedTrieNode(Arrays.asList(mDirMnt, mDirBar),
        false, false);
    Assert.assertEquals(Sets.newHashSet(dirBaz), nodeBar.childrenKeys());
    TrieNode<InodeView> nodeBaz = nodeBar.child(new EmptyInode("baz"));
    Assert.assertEquals(Sets.newHashSet(dirOkc), nodeBaz.childrenKeys());

    root.lowestMatchedTrieNode(Arrays.asList(mDirMnt, mDirBar,
        mDirBaz, mFileBay), true, false);
    Assert.assertEquals(Sets.newHashSet(mDirBaz), nodeBar.childrenKeys());
  }
}
