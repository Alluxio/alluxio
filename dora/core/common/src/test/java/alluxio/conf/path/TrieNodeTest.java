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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests {@link PrefixPathMatcher}.
 */
public class TrieNodeTest {
  @Test
  public void hasTerminalIncludeChildren() {
    TrieNode<Object> node = new TrieNode<>();
    node.insert("/a/b");
    Assert.assertTrue(node.hasTerminal("/", true));
    Assert.assertFalse(node.hasTerminal("/c", true));
    Assert.assertTrue(node.hasTerminal("/a", true));
    Assert.assertTrue(node.hasTerminal("/a/b", true));
    Assert.assertTrue(node.hasTerminal("/a/b/c", true));
  }

  @Test
  public void hasTerminal() {
    TrieNode<Object> node = new TrieNode<>();
    node.insert("/a/b");
    Assert.assertFalse(node.hasTerminal("/", false));
    Assert.assertFalse(node.hasTerminal("/c", false));
    Assert.assertFalse(node.hasTerminal("/a", false));
    Assert.assertTrue(node.hasTerminal("/a/b", false));
    Assert.assertTrue(node.hasTerminal("/a/b/c", false));
  }

  @Test
  public void getCommonRoots() {
    TrieNode<Object> node = new TrieNode<>();
    TrieNode<Object> a = node.insert("/a");
    TrieNode<Object> b = node.insert("/a/b");
    TrieNode<Object> f = node.insert("/a/e/f");
    TrieNode<Object> d = node.insert("/c/d");
    TrieNode<Object> g = node.insert("/c/g");
    Set<TrieNode<Object>> roots = Streams.stream(node.getCommonRoots()).collect(Collectors.toSet());
    Assert.assertTrue(roots.contains(a));
    Assert.assertFalse(roots.contains(b));
    Assert.assertTrue(roots.contains(d));
    Assert.assertFalse(roots.contains(f));
    Assert.assertTrue(roots.contains(g));
  }

  @Test
  public void searchExact() {
    TrieNode<Object> node = new TrieNode<>();
    TrieNode<Object> a = node.insert("/a");
    TrieNode<Object> b = node.insert("/a/b");
    TrieNode<Object> f = node.insert("/a/e/f");
    TrieNode<Object> d = node.insert("/c/d");
    TrieNode<Object> g = node.insert("/c/g");
    TrieNode<Object> h = node.insert("/u/h");
    Assert.assertEquals(a, node.searchExact("/a").get());
    Assert.assertEquals(b, node.searchExact("/a/b").get());
    Assert.assertEquals(f, node.searchExact("/a/e/f").get());
    Assert.assertEquals(d, node.searchExact("/c/d").get());
    Assert.assertEquals(g, node.searchExact("/c/g").get());
    Assert.assertEquals(h, node.searchExact("/u/h").get());
    Assert.assertEquals(Optional.empty(), node.searchExact("/"));
    Assert.assertEquals(Optional.empty(), node.searchExact("/ab"));
    Assert.assertEquals(Optional.empty(), node.searchExact("/a/b/c"));
    Assert.assertEquals(Optional.empty(), node.searchExact("/a/d"));
  }

  @Test
  public void deleteIfTrue() {
    TrieNode<Object> node = new TrieNode<>();
    TrieNode<Object> a = node.insert("/a");
    TrieNode<Object> b = node.insert("/a/b");
    TrieNode<Object> f = node.insert("/a/e/f");
    TrieNode<Object> d = node.insert("/c/d");
    TrieNode<Object> g = node.insert("/c/g");
    TrieNode<Object> h = node.insert("/u/h");
    Assert.assertTrue(node.search("/a/b").contains(b));
    TrieNode<Object> b2 = node.deleteIf("/a/b", n -> {
      Assert.assertEquals(b, n);
      return true;
    });
    Assert.assertEquals(b, b2);
    Assert.assertFalse(node.search("/a/b").contains(b));
    Assert.assertTrue(node.search("/a").contains(a));
    TrieNode<Object> a2 = node.deleteIf("/a", n -> {
      Assert.assertEquals(a, n);
      return true;
    });
    Assert.assertEquals(a, a2);
    Assert.assertFalse(node.search("/a").contains(a));
    Assert.assertTrue(node.search("/a/e/f").contains(f));
    TrieNode<Object> c2 = node.deleteIf("/c", n -> true);
    Assert.assertNull(c2);
    Assert.assertTrue(node.search("/c/d").contains(d));
    Assert.assertTrue(node.search("/c/g").contains(g));
    TrieNode<Object> h2 = node.deleteIf("/u/h", n -> {
      Assert.assertEquals(h, n);
      return true;
    });
    Assert.assertEquals(h, h2);
    TrieNode<Object> nil = node.deleteIf("/n", n -> {
      Assert.fail();
      return true;
    });
    Assert.assertNull(nil);
  }

  @Test
  public void deleteIfFalse() {
    TrieNode<Object> node = new TrieNode<>();
    TrieNode<Object> a = node.insert("/a");
    TrieNode<Object> b = node.insert("/a/b");
    Assert.assertTrue(node.search("/a/b").contains(b));
    TrieNode<Object> b2 = node.deleteIf("/a/b", n -> false);
    Assert.assertNull(b2);
    Assert.assertTrue(node.search("/a").contains(a));
    TrieNode<Object> a2 = node.deleteIf("/a", n -> false);
    Assert.assertNull(a2);
  }

  @Test
  public void deleteAndInsert() {
    TrieNode<Object> node = new TrieNode<>();
    TrieNode<Object> a = node.insert("/a");
    TrieNode<Object> b = node.insert("/a/b");

    Assert.assertTrue(node.search("/a/b").contains(b));
    TrieNode<Object> b2 = node.deleteIf("/a/b", n -> {
      Assert.assertEquals(b, n);
      return true;
    });
    Assert.assertEquals(b, b2);
    Assert.assertFalse(node.search("/a/b").contains(b));
    TrieNode<Object> b3 = node.insert("/a/b");
    Assert.assertTrue(node.search("/a/b").contains(b3));

    Assert.assertTrue(node.search("/a").contains(a));
    Assert.assertTrue(node.search("/a/b").contains(a));
    TrieNode<Object> a2 = node.deleteIf("/a", n -> {
      Assert.assertEquals(a, n);
      return true;
    });
    Assert.assertEquals(a, a2);
    Assert.assertFalse(node.search("/a/b").contains(a));
    Assert.assertFalse(node.search("/a").contains(a));
    Assert.assertTrue(node.search("/a/b").contains(b3));
    TrieNode<Object> a3 = node.insert("/a");
    Assert.assertTrue(node.search("/a/b").contains(b3));
    Assert.assertTrue(node.search("/a").contains(a3));
    Assert.assertTrue(node.search("/a/b").contains(a3));
  }

  @Test
  public void getChildren() {
    TrieNode<Object> node = new TrieNode<>();
    TrieNode<Object> a = node.insert("/a");
    TrieNode<Object> b = node.insert("/a/b");
    TrieNode<Object> f = node.insert("/a/e/f");
    TrieNode<Object> d = node.insert("/c/d");
    TrieNode<Object> g = node.insert("/c/g");
    TrieNode<Object> h = node.insert("/u/h");
    Assert.assertArrayEquals(new TrieNode[] {a, b, f},
        node.getLeafChildren("/a").toArray(TrieNode[]::new));
    Assert.assertArrayEquals(new TrieNode[] {b},
        node.getLeafChildren("/a/b").toArray(TrieNode[]::new));
    Assert.assertArrayEquals(new TrieNode[] {f},
        node.getLeafChildren("/a/e/f").toArray(TrieNode[]::new));
    Assert.assertArrayEquals(new TrieNode[] {d},
        node.getLeafChildren("/c/d").toArray(TrieNode[]::new));
    Assert.assertEquals(new HashSet(Arrays.asList(a, b, f, d, g, h)),
        node.getLeafChildren("/").collect(Collectors.toSet()));
  }

  @Test
  public void clearTrie() {
    TrieNode<Object> node = new TrieNode<>();
    TrieNode<Object> a = node.insert("/a");
    TrieNode<Object> b = node.insert("/a/b");
    TrieNode<Object> f = node.insert("/a/e/f");
    TrieNode<Object> d = node.insert("/c/d");
    TrieNode<Object> g = node.insert("/c/g");
    TrieNode<Object> h = node.insert("/u/h");

    node.clear();
    // after clearing, each node should only contain itself
    for (TrieNode<Object> nxt : ImmutableList.of(a, b, f, d, g, h)) {
      Assert.assertEquals(Collections.singletonList(nxt),
          nxt.getLeafChildren("/").collect(Collectors.toList()));
    }
  }
}
