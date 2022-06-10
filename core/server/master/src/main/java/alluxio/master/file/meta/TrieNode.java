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

import alluxio.collections.Pair;

import com.amazonaws.annotation.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

/**
 * TrieNode implements the Trie based on given type.
 *
 * It can be used in circumstances related to alluxio/ufs paths matching.
 * @param <T> the underlying type of the TrieNode
 */
@NotThreadSafe
public class TrieNode<T> {

  // mChildren stores the map from T to the child TrieNode of its children
  private final Map<T, TrieNode<T>> mChildren = new HashMap<>();

  // mIsTerminal is true if the path is the last TrieNode of an inserted path
  private boolean mIsTerminal = false;

  /**
   * insert a list of inodes under current TrieNode.
   *
   * @param inodes        inodes to be added to TrieNode
   * @return the last created TrieNode based on inodes
   */
  public TrieNode<T> insert(List<T> inodes) {
    TrieNode<T> current = this;
    for (T inode : inodes) {
      current.mChildren.computeIfAbsent(inode, n -> new TrieNode<>());
      current = current.mChildren.get(inode);
    }
    current.mIsTerminal = true;

    return current;
  }

  /**
   * insert nodes and apply the predicate while traversing the TrieNode tree.
   * @param nodes the nodes to be inserted
   * @param predicate the predicate executed during traversing the existing TrieNodes
   * @return the last created TrieNode based on inodes
   */
  public TrieNode<T> insert(List<T> nodes,
      java.util.function.BiFunction<TrieNode<T>, T, Boolean> predicate) {
    TrieNode<T> current = this;
    for (T node : nodes) {
      if (!current.mChildren.containsKey(node) && !predicate.apply(current, node)) {
        current.mChildren.put(node, new TrieNode<>());
      }
      current = current.mChildren.get(node);
    }
    current.mIsTerminal = true;
    return current;
  }

  /**
   * find the lowest matched TrieNode of given inodes.
   *
   * @param inodes          the target inodes
   * @param isOnlyTerminalNode true if the matched inodes must also be terminal nodes
   * @param isCompleteMatch true if the TrieNode must completely match the given inodes
   * @return null if there is no valid TrieNode, else return the lowest matched TrieNode
   */
  public TrieNode<T> lowestMatchedTrieNode(
      List<T> inodes, boolean isOnlyTerminalNode, boolean isCompleteMatch) {
    TrieNode<T> matchedPos = null;
    TrieNode<T> current = this;
    if (!isCompleteMatch && current.checkNodeTerminal(isOnlyTerminalNode)) {
      matchedPos = current;
    }
    for (int i = 0; i < inodes.size(); i++) {
      T inode = inodes.get(i);
      if (!current.mChildren.containsKey(inode)) {
        if (isCompleteMatch) {
          return null;
        }
        break;
      }
      current = current.mChildren.get(inode);
      if (current.checkNodeTerminal(isOnlyTerminalNode)
          && (!isCompleteMatch || i == inodes.size() - 1)) {
        matchedPos = current;
      }
    }
    return matchedPos;
  }

  /**
   * find the lowest matched TrieNode of given inodes, the given predicate will be executed.
   *
   * @param inodes          the target inodes
   * @param isOnlyTerminalNode true if the matched inodes must also be terminal nodes
   * @param predicate       executed while traversing the inodes
   * @param isCompleteMatch true if the TrieNode must completely match the given inodes
   * @return null if there is no valid TrieNode, else return the lowest matched TrieNode
   */
  public TrieNode<T> lowestMatchedTrieNode(
      List<T> inodes, boolean isOnlyTerminalNode,
      java.util.function.BiFunction<TrieNode<T>, T, Boolean> predicate, boolean isCompleteMatch) {
    TrieNode<T> current = this;
    TrieNode<T> matchedPos = null;
    if (!isCompleteMatch && current.checkNodeTerminal(isOnlyTerminalNode)) {
      matchedPos = current;
    }
    for (int i = 0; i < inodes.size(); i++) {
      T inode = inodes.get(i);
      if (!current.mChildren.containsKey(inode) && !predicate.apply(current, inode)) {
        if (isCompleteMatch) {
          return null;
        }
        break;
      }
      current = current.mChildren.get(inode);
      if (current.checkNodeTerminal(isOnlyTerminalNode)
          && (!isCompleteMatch || i == inodes.size() - 1)) {
        matchedPos = current;
      }
    }
    return matchedPos;
  }

  /**
   * acquire the direct children's keys.
   * @return key set of the direct children of current TrieNode
   */
  public Collection<T> childrenKeys() {
    return mChildren.keySet();
  }

  /**
   * remove child TrieNode according to the given key.
   * @param key the target TrieNode's key
   */
  public void removeChild(T key) {
    mChildren.remove(key);
  }

  /**
   * add child to the current TrieNode.
   * @param key the target key
   * @param value the target value(TrieNode)
   */
  public void addChild(T key, TrieNode<T> value) {
    mChildren.put(key, value);
  }

  /**
   * get the child TrieNode by given key.
   * @param key the given key to get the corresponding child
   * @return the corresponding child TrieNode
   */
  public TrieNode<T> child(T key) {
    return mChildren.get(key);
  }

  /**
   * acquire all descendant TrieNodes.
   *
   * @param isNodeMustTerminal true if the descendant node must also be a terminal node
   * @param isContainSelf true if the results can contain itself
   * @return all the children TrieNodes
   */
  public List<TrieNode<T>> descendants(boolean isNodeMustTerminal, boolean isContainSelf) {
    List<TrieNode<T>> childrenNodes = new ArrayList<>();
    Queue<TrieNode<T>> queue = new LinkedList<>();
    queue.add(this);
    while (!queue.isEmpty()) {
      TrieNode<T> front = queue.poll();
      if (front.checkNodeTerminal(isNodeMustTerminal) && (isContainSelf || front != this)) {
        childrenNodes.add(front);
      }
      for (Map.Entry<T, TrieNode<T>> entry : front.mChildren.entrySet()) {
        TrieNode<T> value = entry.getValue();
        queue.add(value);
      }
    }
    return childrenNodes;
  }

  /**
   * Checks if current TrieNode contains the certain type of TrieNode.
   *
   * @param isContainSelf true if the results may contain current TrieNode
   * @return true if current TrieNode has children that match the given filters
   */
  public boolean hasNestedTerminalTrieNodes(boolean isContainSelf) {
    if (this.isTerminal() && isContainSelf) {
      return true;
    }
    if (isLastTrieNode()) {
      return false;
    }
    Queue<TrieNode<T>> queue = new LinkedList<>();
    queue.add(this);
    while (!queue.isEmpty()) {
      TrieNode<T> front = queue.poll();
      if (front.isTerminal() && (isContainSelf || front != this)) {
        return true;
      }
      for (Map.Entry<T, TrieNode<T>> entry : front.mChildren.entrySet()) {
        TrieNode<T> value = entry.getValue();
        queue.add(value);
      }
    }
    return false;
  }

  /**
   * Removes the given inodes from current TrieNode.
   *
   * @param inodes    inodes of the path to be removed
   * @return not null if the inodes are removed successfully, else return null
   */
  public TrieNode<T> remove(List<T> inodes) {
    Stack<Pair<TrieNode<T>, T>> parents = new Stack<>();
    TrieNode<T> current = this;
    for (T inode : inodes) {
      if (!current.mChildren.containsKey(inode)) {
        return null;
      }
      parents.push(new Pair<>(current, inode));
      current = current.mChildren.get(inode);
    }
    // We only remove the terminal node
    if (!current.isTerminal()) {
      return null;
    }
    TrieNode<T> nodeToRemove = current;
    current.mIsTerminal = false;
    while (current.isLastTrieNode() && !current.mIsTerminal && !parents.empty()) {
      Pair<TrieNode<T>, T> parent = parents.pop();
      current = parent.getFirst();
      current.mChildren.remove(parent.getSecond());
    }
    return nodeToRemove;
  }

  /**
   * Checks whether current TrieNode is the last one along the TrieNode path.
   *
   * @return true if current TrieNode is the last one along the path
   */
  public boolean isLastTrieNode() {
    return mChildren.isEmpty();
  }

  /**
   * Checks whether current TrieNode is a valid path.
   *
   * @return true if current TrieNode is created via insert
   */
  public boolean isTerminal() {
    return mIsTerminal;
  }

  private boolean checkNodeTerminal(boolean isNodeMustTerminal) {
    return !isNodeMustTerminal || isTerminal();
  }
}
