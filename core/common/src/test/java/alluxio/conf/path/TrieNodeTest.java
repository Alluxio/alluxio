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

package alluxio.conf.path;

import com.google.common.collect.Streams;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests {@link PrefixPathMatcher}.
 */
public class TrieNodeTest {
  @Test
  public void hasTerminalIncludeChildren() {
    TrieNode node = new TrieNode();
    node.insert("/a/b");
    Assert.assertTrue(node.hasTerminal("/", true));
    Assert.assertFalse(node.hasTerminal("/c", true));
    Assert.assertTrue(node.hasTerminal("/a", true));
    Assert.assertTrue(node.hasTerminal("/a/b", true));
    Assert.assertTrue(node.hasTerminal("/a/b/c", true));
  }

  @Test
  public void hasTerminal() {
    TrieNode node = new TrieNode();
    node.insert("/a/b");
    Assert.assertFalse(node.hasTerminal("/", false));
    Assert.assertFalse(node.hasTerminal("/c", false));
    Assert.assertFalse(node.hasTerminal("/a", false));
    Assert.assertTrue(node.hasTerminal("/a/b", false));
    Assert.assertTrue(node.hasTerminal("/a/b/c", false));
  }

  @Test
  public void getCommonRoots() {
    TrieNode node = new TrieNode();
    TrieNode a = node.insert("/a");
    TrieNode b = node.insert("/a/b");
    TrieNode f = node.insert("/a/e/f");
    TrieNode d = node.insert("/c/d");
    TrieNode g = node.insert("/c/g");
    Set<TrieNode> roots = Streams.stream(node.getCommonRoots()).collect(Collectors.toSet());
    Assert.assertTrue(roots.contains(a));
    Assert.assertFalse(roots.contains(b));
    Assert.assertTrue(roots.contains(d));
    Assert.assertFalse(roots.contains(f));
    Assert.assertTrue(roots.contains(g));
  }

  @Test
  public void deleteIfTrue() {
    TrieNode node = new TrieNode();
    TrieNode a = node.insert("/a");
    TrieNode b = node.insert("/a/b");
    TrieNode f = node.insert("/a/e/f");
    TrieNode d = node.insert("/c/d");
    TrieNode g = node.insert("/c/g");
    TrieNode h = node.insert("/u/h");
    Assert.assertTrue(node.search("/a/b").contains(b));
    TrieNode b2 = node.deleteIf("/a/b", n -> {
      Assert.assertEquals(b, n);
      return true;
    });
    Assert.assertEquals(b, b2);
    Assert.assertFalse(node.search("/a/b").contains(b));
    Assert.assertTrue(node.search("/a").contains(a));
    TrieNode a2 = node.deleteIf("/a", n -> {
      Assert.assertEquals(a, n);
      return true;
    });
    Assert.assertEquals(a, a2);
    Assert.assertFalse(node.search("/a").contains(a));
    Assert.assertTrue(node.search("/a/e/f").contains(f));
    TrieNode c2 = node.deleteIf("/c", n -> true);
    Assert.assertNull(c2);
    Assert.assertTrue(node.search("/c/d").contains(d));
    Assert.assertTrue(node.search("/c/g").contains(g));
    TrieNode h2 = node.deleteIf("/u/h", n -> {
      Assert.assertEquals(h, n);
      return true;
    });
    Assert.assertEquals(h, h2);
    TrieNode nil = node.deleteIf("/n", n -> {
      Assert.fail();
      return true;
    });
    Assert.assertNull(nil);
  }

  @Test
  public void deleteIfFalse() {
    TrieNode node = new TrieNode();
    TrieNode a = node.insert("/a");
    TrieNode b = node.insert("/a/b");
    Assert.assertTrue(node.search("/a/b").contains(b));
    TrieNode b2 = node.deleteIf("/a/b", n -> false);
    Assert.assertNull(b2);
    Assert.assertTrue(node.search("/a").contains(a));
    TrieNode a2 = node.deleteIf("/a", n -> false);
    Assert.assertNull(a2);
  }

  @Test
  public void deleteAndInsert() {
    TrieNode node = new TrieNode();
    TrieNode a = node.insert("/a");
    TrieNode b = node.insert("/a/b");

    Assert.assertTrue(node.search("/a/b").contains(b));
    TrieNode b2 = node.deleteIf("/a/b", n -> {
      Assert.assertEquals(b, n);
      return true;
    });
    Assert.assertEquals(b, b2);
    Assert.assertFalse(node.search("/a/b").contains(b));
    TrieNode b3 = node.insert("/a/b");
    Assert.assertTrue(node.search("/a/b").contains(b3));

    Assert.assertTrue(node.search("/a").contains(a));
    Assert.assertTrue(node.search("/a/b").contains(a));
    TrieNode a2 = node.deleteIf("/a", n -> {
      Assert.assertEquals(a, n);
      return true;
    });
    Assert.assertEquals(a, a2);
    Assert.assertFalse(node.search("/a/b").contains(a));
    Assert.assertFalse(node.search("/a").contains(a));
    Assert.assertTrue(node.search("/a/b").contains(b3));
    TrieNode a3 = node.insert("/a");
    Assert.assertTrue(node.search("/a/b").contains(b3));
    Assert.assertTrue(node.search("/a").contains(a3));
    Assert.assertTrue(node.search("/a/b").contains(a3));
  }
}
