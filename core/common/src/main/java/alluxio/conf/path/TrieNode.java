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

import alluxio.collections.Pair;

import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A node in a trie.
 * @param <V> the type of the value held by each node
 */
public final class TrieNode<V> {
  private final Map<String, TrieNode<V>> mChildren = new HashMap<>();
  /**
   * A node is terminal if it is the last visited node when inserting a path.
   */
  private boolean mIsTerminal = false;
  private V mValue;

  /**
   * Set the value associated with this node.
   * @param value the value
   */
  public void setValue(V value) {
    mValue = value;
  }

  /**
   * @return the value associated with this node
   */
  public V getValue() {
    return mValue;
  }

  /**
   * Inserts a path into the trie.
   * Each path component forms a node in the trie,
   * root path "/" will correspond to the root of the trie.
   *
   * @param path a path with components separated by "/"
   * @return the last inserted trie node or the last traversed trie node if no node is inserted
   */
  public TrieNode<V> insert(String path) {
    TrieNode<V> current = this;
    for (String component : path.split("/")) {
      if (!current.mChildren.containsKey(component)) {
        current.mChildren.put(component, new TrieNode<>());
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
  public List<TrieNode<V>> search(String path) {
    List<TrieNode<V>> terminal = new ArrayList<>();
    TrieNode<V> current = this;
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

  /**
   * Find terminal component of the full path if one exists.
   * @param path the path
   * @return the terminal component
   */
  public Optional<TrieNode<V>> searchExact(String path) {
    TrieNode<V> current = this;
    String[] components = path.split("/");
    int i;
    for (i = 0; i < components.length; i++) {
      if (current.mChildren.containsKey(components[i])) {
        current = current.mChildren.get(components[i]);
      } else {
        break;
      }
    }
    if (i == components.length && current.mIsTerminal) {
      return Optional.of(current);
    }
    return Optional.empty();
  }

  /**
   * Checks whether the path has terminal nodes as parents or children.
   *
   * @param path the target path
   * @param includeChildren whether the check should succeed if the path has children terminal nodes
   * @return the terminal nodes sorted by the time they are visited
   */
  public boolean hasTerminal(String path, boolean includeChildren) {
    TrieNode<V> current = this;
    if (current.mIsTerminal) {
      return true;
    }
    for (String component : path.split("/")) {
      TrieNode<V> child = current.mChildren.get(component);
      if (child != null) {
        current = child;
        if (current.mIsTerminal) {
          return true;
        }
      } else {
        return false;
      }
    }
    return includeChildren;
  }

  /**
   * Deletes the path from the Trie if the given predicate is true.
   *
   * @param path the target path
   * @param predicate a predicate to decide whether the node should be deleted or not
   * @return the removed terminal node, or null if the node is not found or not terminal
   */
  public TrieNode<V> deleteIf(String path, java.util.function.Function<TrieNode<V>,
      Boolean> predicate) {
    java.util.Stack<Pair<TrieNode<V>, String>> parents = new java.util.Stack<>();
    TrieNode<V> current = this;
    for (String component : path.split("/")) {
      if (!current.mChildren.containsKey(component)) {
        return null;
      }
      parents.push(new Pair<>(current, component));
      current = current.mChildren.get(component);
    }
    if (!current.mIsTerminal) {
      return null;
    }
    if (!predicate.apply(current)) {
      return null;
    }
    TrieNode<V> nodeToDelete = current;
    current.mIsTerminal = false;
    while (current.mChildren.isEmpty() && !current.mIsTerminal && !parents.empty()) {
      Pair<TrieNode<V>, String> parent = parents.pop();
      current = parent.getFirst();
      current.mChildren.remove(parent.getSecond());
    }
    return nodeToDelete;
  }

  /**
   * @return the iterator of TrieNode that are terminals and have no terminal ancestors
   */
  public Iterator<TrieNode<V>> getCommonRoots() {
    if (mIsTerminal) {
      return Collections.singletonList(this).iterator();
    }
    return Iterators.concat(mChildren.values().stream().map(TrieNode::getCommonRoots).iterator());
  }
}
