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

import java.util.Collection;
import java.util.List;

/**
 * InodeTrieNode extends the TrieNode, overriding two method:
 * - {@link TrieNode#insert(List)}
 * - {@link TrieNode#lowestMatchedTrieNode(List, boolean, boolean)}.
 */
public class InodeTrieNode extends TrieNode<InodeView> {
  /**
   * This method will be passed as a predicate during the traverse of InodeTrie. It accepts a
   * trieNode and an inode as parameters, and it will iterate over all children of the given
   * trieNode, if one of the child is an EmptyInode, and has the same InodeName with the given
   * inode, then the child will be altered as the given inode.
   * @param trieNode the TrieNode that may contain the corresponding EmptyInode of given inode
   * @param inodeToCheck the target inode
   * @return whether substitution happens
   */
  public static boolean checkAndSubstituteEmptyInode(TrieNode<InodeView> trieNode,
                                                     InodeView inodeToCheck) {
    // if inodeToCheck is the instance of EmptyInode, it could not be existed in
    // trieNode's children if we haven't found it via HashMap. Therefore, skip the iteration
    // and return false if inodeToCheck is the instance of EmptyInode.
    if (!(inodeToCheck instanceof EmptyInode)) {
      Collection<InodeView> targetChildInodes = trieNode.childrenKeys();
      // Traverse the children of current TrieNode, see if there is any existing EmptyInode
      // that presents the same inode with inode, substitute it and return true
      for (InodeView existingInode : targetChildInodes) {
        //
        if (existingInode instanceof EmptyInode && existingInode.equals(inodeToCheck)) {
          // acquire the corresponding trieNode
          TrieNode<InodeView> targetTrieNode = trieNode.child(existingInode);
          // remove the existing Map.Entry<EmptyInode, TrieNode> pair
          trieNode.removeChild(existingInode);
          // add the new Map.Entry<InodeView, TrieNode>
          trieNode.addChild(inodeToCheck, targetTrieNode);
          return true;
        }
      }
    }
    // there is no substitution, return false.
    return false;
  }

  /**
   * insert nodes by traversing the TrieNode tree from the root. This method adds a
   * check {@link InodeTrieNode#checkAndSubstituteEmptyInode(TrieNode, InodeView)} on each
   * traversed node.
   * @param nodes the nodes to be inserted
   * @return the last created TrieNode based on nodes
   */
  @Override
  public TrieNode<InodeView> insert(List<InodeView> nodes) {
    TrieNode<InodeView> current = this;
    for (InodeView node : nodes) {
      // check if inode is among current's children, if not, check if predicate
      // is not null and returns true.
      if (!current.mChildren.containsKey(node)
          && !checkAndSubstituteEmptyInode(current, node)) {
        current.mChildren.put(node, new TrieNode<>());
      }
      current = current.mChildren.get(node);
    }
    current.mIsTerminal = true;
    return current;
  }

  /**
   * find the lowest matched TrieNode of given inodes. This method adds a check
   * {@link InodeTrieNode#checkAndSubstituteEmptyInode(TrieNode, InodeView)} on each
   * traversed node.
   *
   * @param inodes the target inodes
   * @param isOnlyTerminalNode true if the matched inodes must also be terminal nodes
   * @param isCompleteMatch true if the TrieNode must completely match the given inodes
   * @return null if there is no valid TrieNode, else return the lowest matched TrieNode
   */
  @Override
  public TrieNode<InodeView> lowestMatchedTrieNode(
      List<InodeView> inodes, boolean isOnlyTerminalNode, boolean isCompleteMatch) {
    TrieNode<InodeView> current = this;
    TrieNode<InodeView> matchedPos = null;
    if (!isCompleteMatch && current.checkNodeTerminal(isOnlyTerminalNode)) {
      matchedPos = current;
    }
    for (int i = 0; i < inodes.size(); i++) {
      InodeView inode = inodes.get(i);
      // check if inode is among current's children, if not, check with
      // checkAndSubstituteEmptyInode.
      if (!current.mChildren.containsKey(inode) && !checkAndSubstituteEmptyInode(current, inode)) {
        // the inode is neither the child of current, nor qualified of the predicate, so mismatch
        // happens.
        if (isCompleteMatch) {
          // isCompleteMatch indicates that there must be no mismatch, so return null directly.
          return null;
        }
        break;
      }
      // set current to the matched children TrieNode
      current = current.mChildren.get(inode);
      // based on the condition of whether strict to terminal node and whether it requires
      // completeMatch, decide whether the current TrieNode is a valid matchedPoint.
      if (current.checkNodeTerminal(isOnlyTerminalNode)
          && (!isCompleteMatch || i == inodes.size() - 1)) {
        matchedPos = current;
      }
    }
    return matchedPos;
  }
}
