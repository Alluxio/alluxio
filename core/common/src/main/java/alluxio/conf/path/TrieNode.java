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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A node in a trie.
 */
public final class TrieNode {
  private Map<String, TrieNode> mChildren = new HashMap<>();
  /**
   * A node is terminal if it is the last visited node when inserting a path.
   */
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
