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

import alluxio.proto.meta.InodeMeta;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link MountTableTrieNode}.
 */
public class TrieNodeTest extends BaseInodeLockingTest {
  @Test
  public void testInsert() {
    MountTableTrieNode root = new MountTableTrieNode();
    root.insert(Arrays.asList(createDir(1L), createDir(2L), createDir(3L)));
    root.insert(Arrays.asList(createDir(1L), createDir(4L), createDir(5L)));
    root.insert(Arrays.asList(createDir(1L), createDir(6L)));
    root.insert(Arrays.asList(createDir(1L), createDir(4L), createDir(9L)));

    MountTableTrieNode node1 = root.lowestMatchedTrieNode(Collections.singletonList(createDir(1L)),
        false, true);

    Assert.assertEquals(Sets.newHashSet(createDir(2L), createDir(4L), createDir(6L)), node1.childrenKeys());
    Assert.assertEquals(Sets.newHashSet(createDir(5L), createDir(9L)), node1.getChild(createDir(4L)).childrenKeys());
    Assert.assertNull(root.getChild(createDir(10L)));
  }

  @Test
  public void testLowestMatchedTrieNode() {
    MountTableTrieNode root = new MountTableTrieNode();

    root.insert(Arrays.asList(createDir(1L), createDir(2L), createDir(3L)));
    root.insert(Arrays.asList(createDir(1L), createDir(4L), createDir(5L)));
    root.insert(Arrays.asList(createDir(1L), createDir(6L)));
    root.insert(Arrays.asList(createDir(1L), createDir(4L), createDir(9L)));

    MountTableTrieNode node1 = root.lowestMatchedTrieNode(Collections.singletonList(createDir(1L)),
        false, false);
    Assert.assertEquals(Sets.newHashSet(createDir(2L), createDir(4L), createDir(6L)), node1.childrenKeys());
    Assert.assertEquals(node1, node1.lowestMatchedTrieNode(new ArrayList<>(), false, false));
    Assert.assertNull(node1.lowestMatchedTrieNode(new ArrayList<>(), true, false));
    Assert.assertNull(node1.lowestMatchedTrieNode(Arrays.asList(createDir(3L), createDir(5L)), true, true));
    Assert.assertNull(node1.lowestMatchedTrieNode(Arrays.asList(createDir(3L), createDir(5L)), true, false));
    Assert.assertNotNull(node1.lowestMatchedTrieNode(Arrays.asList(createDir(3L), createDir(5L)), false, false));
  }

  @Test
  public void testRemove() {
    MountTableTrieNode root = new MountTableTrieNode();

    root.insert(Arrays.asList(createDir(1L), createDir(2L), createDir(3L)));
    root.insert(Arrays.asList(createDir(1L), createDir(4L), createDir(5L)));
    root.insert(Arrays.asList(createDir(1L), createDir(6L)));
    root.insert(Arrays.asList(createDir(1L), createDir(4L), createDir(9L)));

    MountTableTrieNode node1 = root.lowestMatchedTrieNode(Collections.singletonList(createDir(1L)), false,
        true);
    // remove a path that is not existed will return null
    Assert.assertNull(root.remove(Arrays.asList(createDir(1L), createDir(7L), createDir(9L))));
    // remove a non-terminal path will return null
    Assert.assertNull(root.remove(Arrays.asList(createDir(1L), createDir(4L))));
    // remove a terminal path successfully
    Assert.assertNotNull(root.remove(Arrays.asList(createDir(1L), createDir(4L), createDir(9L))));
    // node '4' has child after the remove, so it is still the child of node '1'
    Assert.assertNotNull(node1.getChild(createDir(4L)));
    Assert.assertNotNull(root.remove(Arrays.asList(createDir(1L), createDir(4L), createDir(5L))));
    // node '4' has no child after the remove, so it will be removed by the above remove call
    Assert.assertNull(node1.getChild(createDir(4L)));
  }

  @Test
  public void testDescendents() {
    MountTableTrieNode root = new MountTableTrieNode();

    root.insert(Arrays.asList(createDir(1L), createDir(2L), createDir(3L)));
    root.insert(Arrays.asList(createDir(1L), createDir(4L)));

    MountTableTrieNode node1 = root.lowestMatchedTrieNode(Collections.singletonList(createDir(1L)), false, true);
    MountTableTrieNode node2 = node1.lowestMatchedTrieNode(Collections.singletonList(createDir(2L)), false,
        true);
    MountTableTrieNode node3 = node2.lowestMatchedTrieNode(Collections.singletonList(createDir(3L)), false,
        true);
    MountTableTrieNode node4 = node1.lowestMatchedTrieNode(Collections.singletonList(createDir(4L)), false,
        true);

    Assert.assertEquals(Arrays.asList(node1, node2, node4, node3), root.descendants(false,
        false, false));
    Assert.assertEquals(Arrays.asList(root, node1, node2, node4, node3), root.descendants(false,
        true, false));
    Assert.assertEquals(Arrays.asList(node4, node3), root.descendants(true, true, false));
    Assert.assertTrue(node2.hasNestedTerminalTrieNodes(false));
    Assert.assertFalse(node3.hasNestedTerminalTrieNodes(false));
  }
  private MutableInodeDirectory createDir(long fileId) {
    InodeMeta.InodeOrBuilder builder = InodeMeta.Inode.newBuilder()
        .setId(fileId)
        .setPersistenceState(PersistenceState.PERSISTED.toString())
        .setIsDirectory(true)
        .setDefaultAcl(
            alluxio.proto.shared.Acl.AccessControlList.newBuilder().setIsDefault(true).build());
    return MutableInodeDirectory.fromProto(builder);
  }
}

