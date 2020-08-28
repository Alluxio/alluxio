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
 * A {@link PathMatcher} to match a path against a set of paths.
 *
 * Path a matches path b if a is the same as b or a is in the subtree of b.
 * For example:
 * /a/b/c matches /a/b, but
 * /a/bc does not match /a/b because although /a/b is a prefix of /a/bc, /a/bc is not
 * in the subtree of /a/b.
 */
@ThreadSafe
public final class PrefixPathMatcher implements PathMatcher {
  /**
   * Root of the trie for paths.
   */
  private final TrieNode mTrie = new TrieNode();
  /**
   * A map from terminal trie node to the corresponding path.
   */
  private final Map<TrieNode, String> mPaths = new HashMap<>();

  /**
   * Builds internal trie based on paths.
   *
   * Each path must start with "/".
   *
   * @param paths the list of path
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
    List<TrieNode> nodes = mTrie.search(path.getPath());
    if (nodes.isEmpty()) {
      return Optional.empty();
    }
    List<String> matchedPaths = new ArrayList<>();
    for (int i = nodes.size() - 1; i >= 0; i--) {
      TrieNode node = nodes.get(i);
      if (mPaths.containsKey(node)) {
        matchedPaths.add(mPaths.get(node));
      }
    }
    return Optional.of(matchedPaths);
  }
}
