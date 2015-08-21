/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.next.filesystem.meta;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.master.block.BlockId;
import tachyon.master.next.IndexedSet;
import tachyon.master.next.block.ContainerIdGenerator;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;

public final class InodeTree {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final InodeDirectory mRoot;

  private final IndexedSet.FieldIndex mIdIndex = new IndexedSet.FieldIndex<Inode>() {
    @Override
    public Object getFieldValue(Inode o) {
      return o.getId();
    }
  };
  private final IndexedSet<Inode> mInodes = new IndexedSet<Inode>(mIdIndex);
  /** A set of inode ids representing pinned inode files */
  private final Set<Long> mPinnedInodeFileIds = new HashSet<Long>();

  /**
   * Inode id management. Inode ids are essentially block ids. inode files: Each file id will be
   * composed of a unique block container id, with the maximum sequence number. inode directories:
   * Each directory id will be a unique block id, in order to avoid any collision with file ids.
   */
  private final ContainerIdGenerator mContainerIdGenerator;
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;

  public InodeTree(ContainerIdGenerator containerIdGenerator) {
    mContainerIdGenerator = containerIdGenerator;
    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(containerIdGenerator);

    mRoot = new InodeDirectory("", mDirectoryIdGenerator.getNewDirectoryId(), -1,
        System.currentTimeMillis());
    mInodes.add(mRoot);
  }

  public Inode getInodeById(long id) throws FileDoesNotExistException {
    Inode inode = mInodes.getFirstByField(mIdIndex, id);
    if (inode == null) {
      throw new FileDoesNotExistException("Inode id " + id + " does not exist.");
    }
    return inode;
  }

  public Inode getInodeByPath(TachyonURI path) throws InvalidPathException {
    TraversalResult traversalResult = traverseToInode(PathUtils.getPathComponents(path.toString()));
    if (!traversalResult.isFound()) {
      throw new InvalidPathException("Could not find path: " + path);
    }
    return traversalResult.getInode();
  }

  public boolean isRootId(long id) {
    return id == mRoot.getId();
  }

  public TachyonURI getPath(Inode inode) {
    if (isRootId(inode.getId())) {
      return new TachyonURI(TachyonURI.SEPARATOR);
    }
    if (isRootId(inode.getParentId())) {
      return new TachyonURI(TachyonURI.SEPARATOR + inode.getName());
    }
    return getPath(mInodes.getFirstByField(mIdIndex, inode.getParentId())).join(inode.getName());
  }

  public Inode createPath(TachyonURI path, long blockSizeBytes, boolean recursive,
      boolean directory)
          throws FileAlreadyExistException, BlockInfoException, InvalidPathException {

    if (path.isRoot()) {
      LOG.info("FileAlreadyExistException: " + path);
      throw new FileAlreadyExistException(path.toString());
    }

    if (!directory && blockSizeBytes < 1) {
      throw new BlockInfoException("Invalid block size " + blockSizeBytes);
    }

    LOG.debug("createPath {}", FormatUtils.parametersToString(path));

    String[] pathComponents = PathUtils.getPathComponents(path.toString());
    String name = path.getName();

    String[] parentPath = new String[pathComponents.length - 1];
    System.arraycopy(pathComponents, 0, parentPath, 0, parentPath.length);

    long creationTimeMs = System.currentTimeMillis();

    TraversalResult traversalResult = traverseToInode(parentPath);
    // pathIndex is the index into pathComponents where we start filling in the path from the inode.
    int pathIndex = parentPath.length;
    if (!traversalResult.isFound()) {
      // Then the path component at errorInd k doesn't exist. If it's not recursive, we throw an
      // exception here. Otherwise we add the remaining path components to the list of components
      // to create.
      if (!recursive) {
        final String msg = "File " + path + " creation failed. Component "
            + traversalResult.getNonexistentPathIndex() + "("
            + parentPath[traversalResult.getNonexistentPathIndex()] + ") does not exist";
        LOG.info("InvalidPathException: " + msg);
        throw new InvalidPathException(msg);
      } else {
        // We will start filling at the index of the non-existing step found by the traveral
        pathIndex = traversalResult.getNonexistentPathIndex();
      }
    }

    if (!traversalResult.getInode().isDirectory()) {
      throw new InvalidPathException("Could not traverse to parent directory of path " + path
          + ". Component " + pathComponents[pathIndex - 1] + " is not a directory.");
    }
    InodeDirectory currentInodeDirectory = (InodeDirectory) traversalResult.getInode();
    // Fill in the directories that were missing.
    for (int k = pathIndex; k < parentPath.length; k ++) {
      Inode dir = new InodeDirectory(pathComponents[k], mDirectoryIdGenerator.getNewDirectoryId(),
          currentInodeDirectory.getId(), creationTimeMs);
      dir.setPinned(currentInodeDirectory.isPinned());
      currentInodeDirectory.addChild(dir);
      currentInodeDirectory.setLastModificationTimeMs(creationTimeMs);
      mInodes.add(dir);
      currentInodeDirectory = (InodeDirectory) dir;
    }

    // Create the final path component. First we need to make sure that there isn't already a file
    // here with that name. If there is an existing file that is a directory and we're creating a
    // directory, we just return the existing directory's id.
    Inode ret = currentInodeDirectory.getChild(name);
    if (ret != null) {
      if (ret.isDirectory() && directory) {
        return ret;
      }
      LOG.info("FileAlreadyExistException: " + path);
      throw new FileAlreadyExistException(path.toString());
    }
    if (directory) {
      ret = new InodeDirectory(name, mDirectoryIdGenerator.getNewDirectoryId(),
          currentInodeDirectory.getId(), creationTimeMs);
    } else {
      ret = new InodeFile(name, mContainerIdGenerator.getNewContainerId(),
          currentInodeDirectory.getId(), blockSizeBytes, creationTimeMs);
      if (currentInodeDirectory.isPinned()) {
        // Update set of pinned file ids.
        mPinnedInodeFileIds.add(ret.getId());
      }
    }
    ret.setPinned(currentInodeDirectory.isPinned());

    mInodes.add(ret);
    currentInodeDirectory.addChild(ret);
    currentInodeDirectory.setLastModificationTimeMs(creationTimeMs);

    LOG.debug("createFile: File Created: {} parent: ", ret, currentInodeDirectory);
    return ret;
  }

  /**
   * Returns a list of all descendants of a particular {@link InodeDirectory}. Any directory inode
   * precedes its descendants in the list.
   *
   * @param inodeDirectory The root {@link InodeDirectory} to retrieve all descendants from.
   * @return A list of all descendants.
   */
  public List<Inode> getInodeChildrenRecursive(InodeDirectory inodeDirectory) {
    List<Inode> ret = new ArrayList<Inode>();
    for (Inode i : inodeDirectory.getChildren()) {
      ret.add(i);
      if (i.isDirectory()) {
        ret.addAll(getInodeChildrenRecursive((InodeDirectory) i));
      }
    }
    return ret;
  }

  /**
   * Deletes a single inode from the inode tree by removing it from the parent inode.
   *
   * @param inode The {@link Inode} to delete
   */
  public void deleteInode(Inode inode) throws FileDoesNotExistException {
    InodeDirectory parent = (InodeDirectory) getInodeById(inode.getParentId());
    parent.removeChild(inode);
    parent.setLastModificationTimeMs(System.currentTimeMillis());

    mInodes.remove(inode);
    mPinnedInodeFileIds.remove(inode.getId());
    inode.delete();
  }

  /**
   * Sets the pinned state of an inode. If the inode is a directory, the pinned state will be set
   * recursively.
   *
   * @param inode The {@link Inode} to set the pinned state for.
   * @param pinned The pinned state to set for the inode (and possible descendants).
   */
  public void setPinned(Inode inode, boolean pinned) {
    inode.setPinned(pinned);
    inode.setLastModificationTimeMs(System.currentTimeMillis());

    if (inode.isFile()) {
      if (inode.isPinned()) {
        mPinnedInodeFileIds.add(inode.getId());
      } else {
        mPinnedInodeFileIds.remove(inode.getId());
      }
    } else {
      // inode is a directory. Set the pinned state for all children.
      for (Inode child : ((InodeDirectory) inode).getChildren()) {
        setPinned(child, pinned);
      }
    }
  }

  // TODO: this should return block container ids, not file ids.
  public Set<Long> getPinIdSet() {
    return Sets.newHashSet(mPinnedInodeFileIds);
  }

  private TraversalResult traverseToInode(String[] pathComponents) throws InvalidPathException {
    if (pathComponents == null) {
      throw new InvalidPathException("passed-in pathComponents is null");
    } else if (pathComponents.length == 0) {
      throw new InvalidPathException("passed-in pathComponents is empty");
    } else if (pathComponents.length == 1) {
      if (pathComponents[0].equals("")) {
        return TraversalResult.createFoundResult(mRoot);
      } else {
        throw new InvalidPathException("File name starts with " + pathComponents[0]);
      }
    }

    Inode current = mRoot;

    // iterate from 1, because 0 is root and it's already added
    for (int i = 1; i < pathComponents.length; i ++) {
      Inode next = ((InodeDirectory) current).getChild(pathComponents[i]);
      if (next == null) {
        // The user might want to create the nonexistent directories, so return the traversal result
        // current inode with the last Inode taken, and the index of the first path component that
        // couldn't be found.
        return TraversalResult.createNotFoundResult(current, i);
      } else if (next.isFile()) {
        // The inode can't have any children. If this is the last path component, we're good.
        // Otherwise, we can't traverse further, so we clean up and throw an exception.
        if (i == pathComponents.length - 1) {
          return TraversalResult.createFoundResult(next);
        } else {
          throw new InvalidPathException(
              "Traversal failed. Component " + i + "(" + next.getName() + ") is a file");
        }
      } else {
        // next is a directory and keep navigating
        current = next;
      }
    }
    return TraversalResult.createFoundResult(current);
  }

  private static final class TraversalResult {
    private final boolean mFound;
    /**
     * when the path is not found in a traversal, the index of the first path component that
     * couldn't be found
     */
    private final int mNonexistentIndex;
    /**
     * the found inode when the traversal succeeds; otherwise the last path component navigated
     */
    private final Inode mInode;

    static TraversalResult createFoundResult(Inode inode) {
      return new TraversalResult(true, -1, inode);
    }

    static TraversalResult createNotFoundResult(Inode inode, int nonexistentIndex) {
      return new TraversalResult(false, nonexistentIndex, inode);
    }

    private TraversalResult(boolean found, int nonexistentIndex, Inode inode) {
      mFound = found;
      mNonexistentIndex = nonexistentIndex;
      mInode = inode;
    }

    boolean isFound() {
      return mFound;
    }

    int getNonexistentPathIndex() {
      if (mFound) {
        throw new UnsupportedOperationException("The traversal is successful");
      }
      return mNonexistentIndex;
    }

    Inode getInode() {
      return mInode;
    }
  }
  /**
   * Inode id management for directory inodes. Keep track of a block container id, along with a
   * block sequence number. If the block sequence number reaches the limit, a new block container id
   * is retrieved.
   */
  private static class InodeDirectoryIdGenerator {
    private final ContainerIdGenerator mContainerIdGenerator;
    private long mContainerId;
    private long mSequenceNumber;

    InodeDirectoryIdGenerator(ContainerIdGenerator containerIdGenerator) {
      mContainerIdGenerator = containerIdGenerator;
      mContainerId = mContainerIdGenerator.getNewContainerId();
      mSequenceNumber = 0;
    }

    long getNewDirectoryId() {
      long directoryId = BlockId.createBlockId(mContainerId, mSequenceNumber);
      if (mSequenceNumber == BlockId.getMaxSequenceNumber()) {
        // No more ids in this container. Get a new container for the next id.
        mContainerId = mContainerIdGenerator.getNewContainerId();
        mSequenceNumber = 0;
      } else {
        mSequenceNumber ++;
      }
      return directoryId;
    }
  }

}
