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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

/**
 * MountPointInodeTrieNode implements the Trie based on given type.
 *
 * It can be used in circumstances related to alluxio/ufs paths matching.
 * @param <T> the underlying type of the TrieNode
 */
@NotThreadSafe
public final class MountPointInodeTrieNode<T> {
  // mChildren stores the map from T to the child TrieNode of its children
  private Map<T, MountPointInodeTrieNode<T>> mChildren = new HashMap<>();

  // mList is valid only when mIsTerminal is true
  private List<T> mList = null;

  // mIsMountPoint is set when inserting a list of inodes
  private boolean mIsMountPoint = false;

  // mIsTerminal is true if the path is the last TrieNode of an inserted path
  private boolean mIsTerminal = false;

  /**
   * insert a list of inodes under current TrieNode.
   *
   * @param inodes        inodes to be added to TrieNode
   * @param mIsMountPoint true if inodes describe a mountPath
   * @return the last created TrieNode based on inodes
   */
  public MountPointInodeTrieNode<T> insert(List<T> inodes, boolean mIsMountPoint) {
    MountPointInodeTrieNode<T> current = this;
    for (T inode : inodes) {
      if (!current.mChildren.containsKey(inode)) {
        current.mChildren.put(inode, new MountPointInodeTrieNode<T>());
      }
      current = current.mChildren.get(inode);
    }
    current.mIsMountPoint = mIsMountPoint;
    current.mIsTerminal = true;
    if (mIsTerminal) {
      current.mList = new ArrayList<>(mList);
      current.mList.addAll(inodes);
    } else {
      current.mList = new ArrayList<>(inodes);
    }
    return current;
  }

  /**
   * find the lowest matched TrieNode of given inodes.
   *
   * @param inodes          the target inodes
   * @param predicate       true if this matched TrieNode must also be a mount point
   * @param isCompleteMatch true if the TrieNode must completely match the given inodes
   * @return null if there is no valid TrieNode, else return the lowest matched TrieNode
   */
  public MountPointInodeTrieNode<T> lowestMatchedTrieNode(
      List<T> inodes, java.util.function.Function<MountPointInodeTrieNode, Boolean> predicate,
      boolean isCompleteMatch) {
    MountPointInodeTrieNode<T> matchedPos = null;
    MountPointInodeTrieNode<T> current = this;

    if (inodes.isEmpty() && predicate.apply(this)) {
      return this;
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
      if (predicate.apply(current) && (!isCompleteMatch || i == inodes.size() - 1)) {
        matchedPos = current;
      }
    }
    return matchedPos;
  }

  /**
   * find the child among current TrieNode's direct children that have key as its identifier.
   *
   * @param predicate the filter condition
   * @param key the key of searched child
   * @return not null if the valid child exists, else return null
   */
  public MountPointInodeTrieNode<T> child(T key,
                                          java.util.function.Function<MountPointInodeTrieNode<T>,
                                              Boolean> predicate) {
    if (isLastTrieNode()) {
      return null;
    }
    MountPointInodeTrieNode<T> node = mChildren.get(key);
    if (node != null && predicate.apply(node)) {
      return node;
    }
    return null;
  }

  /**
   * acquire all the children TrieNodes.
   *
   * @param predicate filter the children nodes
   * @param isContainSelf true if the results contain itself
   * @return all the children TrieNodes
   */
  public List<MountPointInodeTrieNode<T>> allChildrenTrieNode(
      java.util.function.Function<MountPointInodeTrieNode<T>, Boolean> predicate,
      boolean isContainSelf) {
    List<MountPointInodeTrieNode<T>> childrenNodes = new ArrayList<>();
    if (isLastTrieNode()) {
      return childrenNodes;
    }
    Queue<MountPointInodeTrieNode<T>> queue = new LinkedList<>();
    queue.add(this);
    while (!queue.isEmpty()) {
      MountPointInodeTrieNode<T> front = queue.poll();
      if (predicate.apply(front) && (isContainSelf || front != this)) {
        childrenNodes.add(front);
      }
      for (Map.Entry<T, MountPointInodeTrieNode<T>> entry : front.mChildren.entrySet()) {
        MountPointInodeTrieNode<T> value = entry.getValue();
        queue.add(value);
      }
    }
    return childrenNodes;
  }

  /**
   * Checks if current TrieNode contains the certain type of TrieNode.
   *
   * @param predicate filter for TrieNodes
   * @param isContainSelf true if the results may contain current TrieNode
   * @return true if current TrieNode has children that match the given filters
   */
  public boolean isContainsCertainTypeOfTrieNodes(
      java.util.function.Function<MountPointInodeTrieNode<T>, Boolean> predicate,
      boolean isContainSelf) {
    if (predicate.apply(this) && isContainSelf) {
      return true;
    }
    if (isLastTrieNode()) {
      return false;
    }
    Queue<MountPointInodeTrieNode<T>> queue = new LinkedList<>();
    queue.add(this);
    while (!queue.isEmpty()) {
      MountPointInodeTrieNode<T> front = queue.poll();
      if (predicate.apply(front) && (isContainSelf || front != this)) {
        return true;
      }
      for (Map.Entry<T, MountPointInodeTrieNode<T>> entry : front.mChildren.entrySet()) {
        MountPointInodeTrieNode<T> value = entry.getValue();
        queue.add(value);
      }
    }
    return false;
  }

  /**
   * Removes the given inodes from current TrieNode.
   *
   * @param inodes    inodes of the path to be removed
   * @param predicate the condition to qualify inodes
   * @return not null if the inodes are removed successfully, else return null
   */
  public MountPointInodeTrieNode<T> remove(
      List<T> inodes, java.util.function.Function<MountPointInodeTrieNode<T>, Boolean> predicate) {
    Stack<Pair<MountPointInodeTrieNode<T>, T>> parents = new Stack<>();
    MountPointInodeTrieNode<T> current = this;
    for (T inode : inodes) {
      if (!current.mChildren.containsKey(inode)) {
        return null;
      }
      parents.push(new Pair<>(current, inode));
      current = current.mChildren.get(inode);
    }
    if (!predicate.apply(current)) {
      return null;
    }
    MountPointInodeTrieNode<T> nodeToRemove = current;
    current.mIsTerminal = false;
    while (current.isLastTrieNode() && !current.mIsTerminal && !parents.empty()) {
      Pair<MountPointInodeTrieNode<T>, T> parent = parents.pop();
      current = parent.getFirst();
      current.mChildren.remove(parent.getSecond());
    }
    return nodeToRemove;
  }

  /**
   * Return a copy of current List, if it is not a terminal path, then return null.
   *
   * @return the copy of current list
   */
  public List<T> list() {
    if (!isTerminal()) {
      return null;
    }
    return new ArrayList<>(mList);
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
   * Checks whether current TrieNode is a mountPoint.
   *
   * @return true if current TrieNode is a mountPoint
   */
  public boolean isMountPoint() {
    return mIsMountPoint;
  }

  /**
   * Checks whether current TrieNode is a valid path.
   *
   * @return true if current TrieNode is created via insert
   */
  public boolean isTerminal() {
    return mIsTerminal;
  }
}
