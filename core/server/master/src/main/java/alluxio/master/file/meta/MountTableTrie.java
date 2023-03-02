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

import alluxio.exception.InvalidPathException;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;



/**
 * MountTableTrie encapsulates some basic operations of TrieNode.
 */
public final class MountTableTrie {
  public static final String ROOT = "/";
  // The root of Trie of current MountTable
  private TrieNode<InodeView> mMountTableRoot;
  // Map from TrieNode to the alluxio path literal
  private Map<TrieNode<InodeView>, String> mMountPointTrieTable;

  /**
   * Constructor of MountTableTrie.
   */
  public MountTableTrie() {
    mMountTableRoot = new TrieNode<>();
    mMountPointTrieTable = new HashMap<>(10);
  }

  /**
   * Inserts the root inode into Trie, and the MountTableTrie will be built.
   * @param rootInode the target root inode
   */
  protected void setRootInode(InodeView rootInode) {
    Preconditions.checkNotNull(mMountTableRoot);
    Preconditions.checkNotNull(mMountPointTrieTable);
    Preconditions.checkArgument(mMountTableRoot.hasNoChildren());

    TrieNode<InodeView> rootTrieInode =
        mMountTableRoot.insert(Collections.singletonList(rootInode));
    mMountPointTrieTable.put(rootTrieInode, ROOT);
  }

  /**
   * Rebuilds the MountTableTrie from inodeTree and existing mount points.
   * @param inodeTree the given inodeTree
   * @param mountPoints the existing mountPoints
   * @throws InvalidPathException can be thrown when calling getInodesByPath
   */
  protected void recoverFromInodeTreeAndMountPoints(InodeTree inodeTree,
      Set<String> mountPoints) throws Exception {
    Preconditions.checkNotNull(inodeTree);
    Preconditions.checkNotNull(mountPoints);
    Preconditions.checkNotNull(inodeTree.getRoot());
    mMountTableRoot = new TrieNode<>();
    mMountPointTrieTable = new HashMap<>(10);
    for (String mountPoint : mountPoints) {
      List<InodeView> inodeViews = inodeTree.getInodesByPath(mountPoint);
      addMountPoint(mountPoint, inodeViews);
    }
  }

  /**
   * Adds a new mountPoint to MountTableRoot
   * @param mountPoint the given mountPoint
   * @param inodeViews the given inodeViews
   */
  protected void addMountPoint(String mountPoint, List<InodeView> inodeViews) {
    Preconditions.checkState(!inodeViews.isEmpty(), "Mount point %s contains no inodes", mountPoint);
    TrieNode<InodeView> node = mMountTableRoot.insert(inodeViews);
    mMountPointTrieTable.put(node, mountPoint);
  }

  /**
   * Remove a TrieNode from MountTableTrie based on the lockedInodePath.
   * @param inodes the target list of inodes
   */
  protected void removeMountPoint(List<InodeView> inodes) {
    Preconditions.checkArgument(inodes != null && !inodes.isEmpty());
    Preconditions.checkNotNull(mMountTableRoot);
    TrieNode<InodeView> trieNode =
        mMountTableRoot.remove(inodes);
    mMountPointTrieTable.remove(trieNode);
  }

  /**
   * Get the mount point of the given inodes.
   * @param inodeViewList the target inodes
   * @return the lowest mountPoint of the given path
   */
  protected String getMountPoint(List<InodeView> inodeViewList) {
    Preconditions.checkNotNull(mMountTableRoot);

    TrieNode<InodeView> res = mMountTableRoot.lowestMatchedTrieNode(inodeViewList,
        true, false);
    return mMountPointTrieTable.get(res);
  }

  /**
   * Finds all the mount point among the children TrieNodes of the given path. It will call
   * {@link MountTableTrie#findChildrenMountPoints(List, boolean)}.
   * @param path the target inodePath
   * @param containsSelf true if the results can contain the TrieNode of the given path
   * @return the qualified children mount points of the target path
   */
  protected List<String> findChildrenMountPoints(LockedInodePath path, boolean containsSelf) {
    return findChildrenMountPoints(path.getInodeViewList(), containsSelf);
  }

  /**
   * Finds all the mount point among the children TrieNodes of the given inodes.
   * @param inodeViewList the target inodes
   * @param containsSelf true if the results can contain the TrieNode of the given path
   * @return the qualified children mount points of the target path
   */
  private List<String> findChildrenMountPoints(List<InodeView> inodeViewList,
      boolean containsSelf) {
    Preconditions.checkNotNull(mMountTableRoot);

    TrieNode<InodeView> trieNode = mMountTableRoot.lowestMatchedTrieNode(inodeViewList,
        false, true);
    if (trieNode == null) {
      return Collections.emptyList();
    }
    List<String> mountPoints = new ArrayList<>();
    List<TrieNode<InodeView>> childrenTrieNodes = trieNode.descendants(true, containsSelf, true);
    for (TrieNode<InodeView> node : childrenTrieNodes) {
      mountPoints.add(mMountPointTrieTable.get(node));
    }
    return mountPoints;
  }

  /**
   * Checks if the given inodes contains children paths that are mountPoint.
   * @param inodeViewList the target inodes
   * @param containsSelf true if the search targets will include the given path
   * @return true if the target inodes contains at least one mountPoint
   */
  protected boolean hasChildrenContainsMountPoints(List<InodeView> inodeViewList,
                                                boolean containsSelf) {
    Preconditions.checkNotNull(mMountTableRoot);
    TrieNode<InodeView> trieNode = mMountTableRoot.lowestMatchedTrieNode(inodeViewList,
        false, true);
    if (trieNode == null) {
      return false;
    }
    return trieNode.hasNestedTerminalTrieNodes(containsSelf);
  }
}
