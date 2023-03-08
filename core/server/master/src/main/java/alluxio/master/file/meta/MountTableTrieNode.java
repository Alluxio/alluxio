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

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
 */
@NotThreadSafe
public class MountTableTrieNode {

  /** mChildren stores the map from T to the child TrieNode of its children. **/
  protected final Map<InodeView, MountTableTrieNode> mChildren = new HashMap<>();

  /**
   * mIsTerminal indicates whether current TrieNode is the last node of an explicitly-inserted
   * list of T.
   *
   * Here `explicitly-inserted` means the list of T is inserted by calling
   * {@link MountTableTrieNode#insert(List)}. For example, T is Integer, we have a root node,
   * and we call `root->insert(Arrays.asList(1,2,3))`. In this case, 1->2->3 is an
   * explicitly-inserted path. So the TrieNode of `3` is the terminal node of this path.
   *
   * On the other hand, `implicitly-inserted` means the list of T has not been the parameter of
   * {@link MountTableTrieNode#insert(List)}, whereas appearing in the Trie struct. Back to the
   * above example, after calling `root->insert(Arrays.asList(1,2,3))`, there will be 3 path in
   * Trie: 1, 1->2, and 1->2->3. 1 and 1->2 are implicitly-inserted, while 1->2->3 is explicitly
   * inserted. So TrieNodes of `1` and `2` are non-terminal node.
   */
  protected boolean mIsTerminal = false;

  /**
   * Inserts nodes by traversing the TrieNode tree from the root.
   * @param nodes the nodes to be inserted
   * @return the last created TrieNode based on nodes
   */
  public MountTableTrieNode insert(List<InodeView> nodes) {
    MountTableTrieNode current = this;
    for (InodeView node : nodes) {
      // check if inode is among current's children
      // it is actually fine if the node is no longer the same node obj (written then read from RocksDB)
      // because the hashCode of an inode is the InodeId
      if (!current.mChildren.containsKey(node)) {
        current.addChild(node, new MountTableTrieNode());
      }
      current = current.getChild(node);
    }
    current.mIsTerminal = true;
    return current;
  }

  /**
   * Finds the lowest matched TrieNode of given inodes.
   *
   * @param inodes the target inodes
   * @param isLeafNodeOnly true if the matched inodes must also be terminal nodes
   * @param isCompleteMatch true if the TrieNode must completely match the given inodes
   * @return null if there is no valid TrieNode, else return the lowest matched TrieNode
   */
  public MountTableTrieNode lowestMatchedTrieNode(
      List<InodeView> inodes, boolean isLeafNodeOnly, boolean isCompleteMatch) {
    MountTableTrieNode current = this;
    MountTableTrieNode matchedPos = null;
    if (!isCompleteMatch && current.checkNodeTerminal(isLeafNodeOnly)) {
      matchedPos = current;
    }
    for (int i = 0; i < inodes.size(); i++) {
      InodeView inode = inodes.get(i);
      // check if inode is among current's children
      // TODO(jiacheng): double check this equality
      if (!current.mChildren.containsKey(inode)) {
        // the inode is neither the child of current, nor qualified of the predicate, so mismatch
        // happens.
        if (isCompleteMatch) {
          // isCompleteMatch indicates that there must be no mismatch, so return null directly.
          return null;
        }
        break;
      }
      // set current to the matched children TrieNode
      current = current.getChild(inode);
      // based on the condition of whether strict to terminal node and whether it requires
      // completeMatch, decide whether the current TrieNode is a valid matchedPoint.
      if (current.checkNodeTerminal(isLeafNodeOnly)
          && (!isCompleteMatch || i == inodes.size() - 1)) {
        matchedPos = current;
      }
    }
    return matchedPos;
  }

  /**
   * Acquires the direct children's keys.
   * @return key set of the direct children of current TrieNode
   */
  public Collection<InodeView> childrenKeys() {
    return Collections.unmodifiableSet(mChildren.keySet());
  }

  /**
   * Removes child TrieNode according to the given key.
   * @param key the target TrieNode's key
   */
  public void removeChild(InodeView key) {
    mChildren.remove(key);
  }

  /**
   * Adds a child to the current TrieNode.
   * @param key the target key
   * @param value the target value(TrieNode)
   */
  public void addChild(InodeView key, MountTableTrieNode value) {
    mChildren.put(key, value);
  }

  /**
   * Gets the child TrieNode by given key.
   * @param key the given key to get the corresponding child
   * @return the corresponding child TrieNode
   */
  public MountTableTrieNode getChild(InodeView key) {
    return mChildren.get(key);
  }

  /**
   * Acquires all descendant TrieNodes.
   *
   * @param isNodeTerminal true if the descendant node must also be a terminal node
   * @param isContainSelf true if the results can contain itself
   * @param terminateAfterAdd true if the search terminates instantly after finding one qualified
   * @return all the children TrieNodes
   */
  public List<MountTableTrieNode> descendants(boolean isNodeTerminal, boolean isContainSelf,
                                                 boolean terminateAfterAdd) {
    List<MountTableTrieNode> childrenNodes = new ArrayList<>();

    // For now, we use BFS to acquire all nested TrieNodes underneath the current TrieNode.
    Queue<MountTableTrieNode> queue = new LinkedList<>();
    queue.add(this);

    while (!queue.isEmpty()) {
      MountTableTrieNode front = queue.poll();
      // checks if the front of the queue can pass both the terminal check, and the check on
      // whether regarding itself as a descendants.
      // TODO(Jiacheng): double check this equality
      if (front.checkNodeTerminal(isNodeTerminal) && (isContainSelf || front != this)) {
        childrenNodes.add(front);
        if (terminateAfterAdd) {
          break;
        }
      }
      // adds all children of front into the queue.
      for (Map.Entry<InodeView, MountTableTrieNode> entry : front.mChildren.entrySet()) {
        MountTableTrieNode value = entry.getValue();
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
    List<MountTableTrieNode> descendants = descendants(true, isContainSelf, true);
    return descendants.size() > 0;
  }

  /**
   * Removes the given nodes from current TrieNode. The given values must correspond to a
   * terminal TrieNode.
   *
   * @param values inodes of the path to be removed
   * @return the removed terminal node if the inodes are removed successfully, else return null
   */
  public MountTableTrieNode remove(List<InodeView> values) {
    // parents store several <TrieNode, T> pairs, each pair contains the parent TrieNode and the
    // value of its child along the given values.
    Stack<Pair<MountTableTrieNode, InodeView>> parents = new Stack<>();
    MountTableTrieNode current = this;
    for (InodeView value : values) {
      // if the inode of corresponding value is not existed in the current TrieNode, it indicates
      // that the given list of values doesn't exist in Trie.
      if (!current.mChildren.containsKey(value)) {
        return null;
      }
      parents.push(new Pair<>(current, value));
      current = current.getChild(value);
    }
    // Since after traversing through from root based on the given values, we find that the
    // final node we reach is not a terminal node, then we are going to do nothing.
    if (!current.isTerminal()) {
      return null;
    }
    MountTableTrieNode nodeToRemove = current;
    current.mIsTerminal = false;

    // when the current has no child nodes, and is not the terminal node, it can be removed.
    while (current.hasNoChildren() && !current.mIsTerminal && !parents.empty()) {
      Pair<MountTableTrieNode, InodeView> parent = parents.pop();
      current = parent.getFirst();
      // remove current from parent's children map by current's value
      current.removeChild(parent.getSecond());
    }
    return nodeToRemove;
  }

  /**
   * Checks whether current TrieNode is the last one along the TrieNode path.
   *
   * @return true if current TrieNode is the last one along the path
   */
  public boolean hasNoChildren() {
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

  /**
   * Returns true if this node is valid.
   * Here, `valid` is defined as either:
   * - this node is not required to be a terminal node
   * or:
   * - this node is required to be a terminal node, and luckily it is.
   *
   * For how to use this method, check
   * {@link MountTableTrieNode#lowestMatchedTrieNode(List, boolean, boolean)}
   *
   * @param isTerminalNodeOnly indicates whether this node is required to be a terminal node.
   * @return true if it is valid.
   */
  protected boolean checkNodeTerminal(boolean isTerminalNodeOnly) {
    return !isTerminalNodeOnly || isTerminal();
  }
}
