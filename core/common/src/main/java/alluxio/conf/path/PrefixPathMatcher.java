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

import alluxio.AlluxioURI;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Matched patterns are prefixes of a path, the path must be in the subtree of the prefixes.
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
    private Map<String, TrieNode> mChildren = new HashMap<>();
    private boolean mIsTerminal = false;

    /**
     * Inserts a path into the trie.
     *
     * Each path component forms a node in the trie,
     * root path "/" will correspond to the root of the trie.
     *
     * @param path a path with components separated by "/"
     * @return the last inserted trie node or the last traversed trie node if no node is inserted
     */
    public TrieNode insert(String path) {
      TrieNode current = this;
      for (String component : path.split("/")) {
        if (!current.mChildren.containsKey(component)) {
          current.mChildren.put(component, new TrieNode());
        }
        current = current.mChildren.get(component);
      }
      current.mIsTerminal = true;
      return current;
    }

    /**
     * Traverses the trie along the path components until the traversal cannot proceed any more.
     *
     * Returns a list of visited terminal node, a node is terminal if it is the last visited
     * node of an inserted path.
     *
     * @param path the target path
     * @return the terminal nodes sorted by the time they are visited
     */
    public List<TrieNode> search(String path) {
      List<TrieNode> terminal = new ArrayList<>();
      TrieNode current = this;
      if (current.mIsTerminal) {
        terminal.add(current);
      }
      for (String component : path.split("/")) {
        if (current.mChildren.containsKey(component)) {
          current = current.mChildren.get(component);
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

  /**
   * Root of the trie for path patterns.
   */
  private final TrieNode mTrie = new TrieNode();
  /**
   * A map from terminal trie node to the corresponding path pattern.
   */
  private final Map<TrieNode, String> mPaths = new HashMap<>();

  /**
   * Builds internal trie based on paths.
   *
   * Each path pattern must start with "/".
   *
   * @param paths the list of path patterns
   */
  public PrefixPathMatcher(Set<String> paths) {
    for (String path : paths) {
      Preconditions.checkArgument(path.startsWith("/"), "Path must start with /");
      TrieNode node = mTrie.insert(path);
      mPaths.put(node, path);
    }
  }

  @Override
  public Optional<List<String>> match(AlluxioURI path) {
    Preconditions.checkArgument(path.getPath().startsWith("/"), "Path must start with /");
    List<TrieNode> nodes = mTrie.search(path.getPath());
    if (nodes.isEmpty()) {
      return Optional.empty();
    }
    List<String> matchedPatterns = new ArrayList<>();
    for (int i = nodes.size() - 1; i >= 0; i--) {
      TrieNode node = nodes.get(i);
      if (mPaths.containsKey(node)) {
        String pattern = mPaths.get(node);
        matchedPatterns.add(pattern);
      }
    }
    return Optional.of(matchedPatterns);
  }
}
