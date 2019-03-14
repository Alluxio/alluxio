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

package alluxio.conf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Matches the path with the longest matched prefix, and the path must be in the subtree of the
 * prefix.
 *
 * For example:
 * /a/b/c matches /a/b, but
 * /a/bc does not match /a/b because although /a/b is a prefix of /a/bc, /a/bc is not
 * in the subtree of /a/b.
 *
 * Thread safety:
 * Must be constructed in a single thread and published safely.
 * After construction, all operations on the internal read only data structures are thread safe.
 */
@ThreadSafe
public final class PrefixPathMatcher implements PathMatcher {
  /**
   * A node in a trie.
   */
  private final class TrieNode {
    private Map<Character, TrieNode> mChildren = new HashMap<>();
    private boolean mIsTerminal = false;

    /**
     * Inserts a string into the trie, each character forms a node in the trie.
     *
     * @param s the string
     * @return the last inserted trie node or the last traversed trie node if no node is inserted
     */
    public TrieNode insert(String s) {
      TrieNode current = this;
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        if (!current.mChildren.containsKey(c)) {
          current.mChildren.put(c, new TrieNode());
        }
        current = current.mChildren.get(c);
      }
      current.mIsTerminal = true;
      return current;
    }

    /**
     * Traverses the trie along the characters in s until the traversal cannot proceed any more.
     * Returns a list of visited terminal node, a node is terminal if it is the last visited
     * node of an inserted string.
     *
     * @param s the target string
     * @return the terminal nodes sorted by the time they are visited
     */
    public List<TrieNode> search(String s) {
      List<TrieNode> terminal = new ArrayList<>();
      TrieNode current = this;
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        if (current.mChildren.containsKey(c)) {
          current = current.mChildren.get(c);
          if (current.mIsTerminal) {
            terminal.add(current);
          }
        } else {
          break;
        }
      }
      return terminal;
    }
  }

  private final TrieNode mTrie = new TrieNode();
  private final Map<TrieNode, String> mPaths = new HashMap<>();

  /**
   * Builds internal data structures based on paths.
   *
   * @param paths the list of path patterns
   */
  public PrefixPathMatcher(Set<String> paths) {
    for (String path : paths) {
      TrieNode node = mTrie.insert(path);
      mPaths.put(node, path);
    }
  }

  @Override
  public String match(String path) {
    List<TrieNode> nodes = mTrie.search(path);
    for (int i = nodes.size() - 1; i >= 0; i--) {
      TrieNode node = nodes.get(i);
      if (node != mTrie && mPaths.containsKey(node)) {
        String prefix = mPaths.get(node);
        if ((prefix.length() == path.length()) ||
            (prefix.charAt(prefix.length() - 1) == '/') ||
            (path.charAt(prefix.length()) == '/')) {
          // path is in the subtree of prefix.
          return prefix;
        }
      }
    }
    return "";
  }
}
