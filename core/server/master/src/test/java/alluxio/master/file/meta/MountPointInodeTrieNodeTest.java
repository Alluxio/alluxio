package alluxio.master.file.meta;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests {@link MountPointInodeTrieNode}
 */
public class MountPointInodeTrieNodeTest {
  @Test
  public void insertAndCheckChildren() {
    MountPointInodeTrieNode<Long> root = new MountPointInodeTrieNode<>();

    List<Long> ids1 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(3L);
        add(4L);
      }
    };
    List<Long> ids2 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
      }
    };
    List<Long> ids3 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(5L);
      }
    };
    List<Long> ids4 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(3L);
      }
    };
    root.insert(ids1, true);
    root.insert(ids2, true);
    root.insert(ids4, true);
    MountPointInodeTrieNode<Long> node1 = root.insert(ids3, true);
    MountPointInodeTrieNode<Long> node2 = root.lowestMatchedTrieNode(ids2, n -> true, true);
    MountPointInodeTrieNode<Long> node3 = root.lowestMatchedTrieNode(ids4, n -> true, true);
    Assert.assertNull(node1.child(6L, n -> true));
    Assert.assertNull(node2.child(4L, n -> true));
    Assert.assertNotNull(node3);
  }

  @Test
  public void removeAndCheck() {
    MountPointInodeTrieNode<Long> root = new MountPointInodeTrieNode<>();
    List<Long> ids0 = new ArrayList<Long>() {
      {
        add(1L);
      }
    };
    List<Long> ids1 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(3L);
        add(4L);
      }
    };
    List<Long> ids2 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(5L);
      }
    };
    List<Long> idsn = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
      }
    };
    List<Long> idsToRemove = new ArrayList<Long>() {
      {
        add(1L);
        add(20L);
      }
    };
    MountPointInodeTrieNode<Long> node = root.insert(ids0, true);
    root.insert(ids1, true);
    root.insert(ids2, true);
    root.insert(idsn, true);
    root.insert(idsToRemove, true);
    List<Long> ids3 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(3L);
      }
    };
    List<Long> ids4 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(5L);
      }
    };
    List<Long> ids5 = new ArrayList<Long>() {
      {
        add(1L);
        add(10L);
      }
    };
    MountPointInodeTrieNode<Long> node1 = root.remove(ids5, n -> true);
    Assert.assertNull(node1);
    MountPointInodeTrieNode<Long> node2 = root.remove(ids3, n -> true);
    Assert.assertNotNull(node2);
    MountPointInodeTrieNode<Long> node3 = root.remove(ids4, n -> true);
    Assert.assertNotNull(node3);
    MountPointInodeTrieNode<Long> node4 = root.remove(idsToRemove,
        MountPointInodeTrieNode::isTerminal);
    Assert.assertNotNull(node4);
    List<Long> ids6 = new ArrayList<Long>() {
      {
        add(1L);
        add(23L);
        add(24L);
      }
    };
    root.insert(ids6, true);
    Assert.assertNotNull(root.remove(ids6, n->true));
  }

  @Test
  public void matchInodes() {
    MountPointInodeTrieNode<Long> root = new MountPointInodeTrieNode<>();
    List<Long> ids1 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(3L);
        add(4L);
        add(5L);
      }
    };
    List<Long> ids2 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(6L);
        add(7L);
      }
    };
    List<Long> ids3 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(3L);
      }
    };
    List<Long> ids4 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(3L);
        add(10L);
      }
    };
    List<Long> ids5 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
      }
    };
    List<Long> ids6 = new ArrayList<Long>() {
      {
        add(1L);
        add(3L);
      }
    };
    List<Long> ids7 = new ArrayList<Long>() {
      {
        add(12L);
        add(13L);
      }
    };
    MountPointInodeTrieNode<Long> n1 = root.insert(ids1, true);
    root.insert(ids2, true);
    root.insert(ids3, false);
    root.insert(ids4, true);
    root.insert(ids5, false);
    MountPointInodeTrieNode<Long> n2 = n1.insert(ids7, true);

    Assert.assertEquals(ids1, n1.list());
    ids1.addAll(ids7);
    Assert.assertEquals(ids1, n2.list());
    MountPointInodeTrieNode<Long> node1 = root.lowestMatchedTrieNode(
        ids3, n -> n.isMountPoint(), true
    );
    MountPointInodeTrieNode<Long> node2 = root.lowestMatchedTrieNode(
        ids4, n -> !n.isMountPoint(), false
    );
    MountPointInodeTrieNode<Long> node3 = root.lowestMatchedTrieNode(
        ids4, n -> !n.isMountPoint(), true
    );
    MountPointInodeTrieNode<Long> node4 = node2.lowestMatchedTrieNode(new ArrayList<>(), n -> true,
        true);
    MountPointInodeTrieNode<Long> node5 = root.lowestMatchedTrieNode(
        ids6, n -> true, true
    );
    MountPointInodeTrieNode<Long> node6 = root.lowestMatchedTrieNode(
        ids5, n -> true, true
    );
    Assert.assertNull(node1);
    Assert.assertNotNull(node2);
    Assert.assertNull(node5);
    Assert.assertNotNull(node6.child(3L, MountPointInodeTrieNode::isTerminal));
    Assert.assertEquals(node2, node4);
    Assert.assertNull(node3);
  }

  @Test
  public void allChildren() {
    List<Long> ids1 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(3L);
        add(4L);
      }
    };
    List<Long> ids2 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(5L);
      }
    };
    List<Long> ids3 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
        add(6L);
      }
    };
    List<Long> ids4 = new ArrayList<Long>() {
      {
        add(1L);
        add(2L);
      }
    };
    MountPointInodeTrieNode<Long> root = new MountPointInodeTrieNode<>();
    MountPointInodeTrieNode<Long> n1 = root.insert(ids1, true);
    root.insert(ids2, true);
    root.insert(ids3, true);
    MountPointInodeTrieNode<Long> n2 = root.insert(ids4, true);

    List<MountPointInodeTrieNode<Long>> n1ChildrenTrieNode =
        n1.allChildrenTrieNode(MountPointInodeTrieNode::isTerminal, false);
    List<MountPointInodeTrieNode<Long>> n2ChildrenTrieNode =
        n2.allChildrenTrieNode(MountPointInodeTrieNode::isTerminal, false);

    Assert.assertEquals(new ArrayList<List<Long>>() {{
                        }},
        n1ChildrenTrieNode.stream().map(MountPointInodeTrieNode::list).collect(Collectors.toList()));
    Assert.assertEquals(new ArrayList<List<Long>>() {
      {
        add(ids2);
        add(ids3);
        add(ids1);
      }
    }, n2ChildrenTrieNode.stream().map(MountPointInodeTrieNode::list).collect(Collectors.toList()));
    Assert.assertNull(n2.child(5L, n -> !n.isTerminal()));
  }
}
