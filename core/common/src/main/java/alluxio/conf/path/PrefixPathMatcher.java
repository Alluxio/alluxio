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
 * Patterns to be matched against are paths, a path matches a pattern if it is the same as the
 * pattern or in the subtree of the pattern.
 *
 * For example:
 * /a/b/c matches /a/b, but
 * /a/bc does not match /a/b because although /a/b is a prefix of /a/bc, /a/bc is not
 * in the subtree of /a/b.
 */
@ThreadSafe
public final class PrefixPathMatcher implements PathMatcher {
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
