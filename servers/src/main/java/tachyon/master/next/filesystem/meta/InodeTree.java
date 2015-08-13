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
import tachyon.Pair;
import tachyon.TachyonURI;
import tachyon.master.next.IndexedSet;
import tachyon.master.next.block.meta.BlockId;
import tachyon.master.next.block.ContainerIdGenerator;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;

public class InodeTree {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private InodeDirectory mRoot;

  private final IndexedSet.FieldIndex mIdIndex = new IndexedSet.FieldIndex("mId");
  private final IndexedSet<Inode> mInodes = new IndexedSet<Inode>(mIdIndex);
  /** A set of inode ids representing pinned inode files */
  private final Set<Long> mPinnedInodeFileIds = new HashSet<Long>();

  /**
   * Inode id management. Inode ids are essentially block ids.
   * inode files: Each file id will be composed of a unique block container id, with the maximum
   *              sequence number.
   * inode directories: Each directory id will be a unique block id, in order to avoid any collision
   *                    with file ids.
   */
  private final ContainerIdGenerator mContainerIdGenerator;
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;

  public InodeTree(ContainerIdGenerator containerIdGenerator) {
    mContainerIdGenerator = containerIdGenerator;
    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(containerIdGenerator);

    // TODO
  }

  public Inode getInodeById(long id) {
    return mInodes.getFirst(mIdIndex, id);
  }

  public Inode getInodeByPath(TachyonURI path) throws InvalidPathException {
    return getInodeByPath(PathUtils.getPathComponents(path.toString()));
  }

  private Inode getInodeByPath(String[] pathComponents) throws InvalidPathException {
    Pair<Inode, Integer> inodeTraversal = traverseToInode(pathComponents);
    if (!traversalSucceeded(inodeTraversal)) {
      return null;
    }
    return inodeTraversal.getFirst();
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
    return getPath(getInodeById(inode.getParentId())).join(inode.getName());
  }

  public Inode createPath(TachyonURI path, long blockSizeBytes, boolean recursive,
      boolean directory) throws FileAlreadyExistException, BlockInfoException,
      InvalidPathException {

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

    Pair<Inode, Integer> inodeTraversal = traverseToInode(parentPath);
    // pathIndex is the index into pathComponents where we start filling in the path from the inode.
    int pathIndex = parentPath.length;
    if (!traversalSucceeded(inodeTraversal)) {
      // Then the path component at errorInd k doesn't exist. If it's not recursive, we throw an
      // exception here. Otherwise we add the remaining path components to the list of components
      // to create.
      if (!recursive) {
        final String msg =
            "File " + path + " creation failed. Component " + inodeTraversal.getSecond() + "("
                + parentPath[inodeTraversal.getSecond()] + ") does not exist";
        LOG.info("InvalidPathException: " + msg);
        throw new InvalidPathException(msg);
      } else {
        // We will start filling in the path from inodeTraversal.getSecond()
        pathIndex = inodeTraversal.getSecond();
      }
    }

    if (!inodeTraversal.getFirst().isDirectory()) {
      throw new InvalidPathException("Could not traverse to parent directory of path " + path
          + ". Component " + pathComponents[pathIndex - 1] + " is not a directory.");
    }
    InodeDirectory currentInodeDirectory = (InodeDirectory) inodeTraversal.getFirst();
    // Fill in the directories that were missing.
    for (int k = pathIndex; k < parentPath.length; k ++) {
      Inode dir =
          new InodeDirectory(pathComponents[k], mDirectoryIdGenerator.getNewDirectoryId(),
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
      ret =
          new InodeDirectory(name, mDirectoryIdGenerator.getNewDirectoryId(),
              currentInodeDirectory.getId(), creationTimeMs);
    } else {
      ret =
          new InodeFile(name, mContainerIdGenerator.getNewContainerId(),
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
  public void deleteInode(Inode inode) {
    InodeDirectory parent = (InodeDirectory) getInodeById(inode.getParentId());
    parent.removeChild(inode);
    parent.setLastModificationTimeMs(System.currentTimeMillis());

    mInodes.remove(inode);
    mPinnedInodeFileIds.remove(inode.getId());
    inode.reverseId();
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



  // TODO: make this into a result class
  private boolean traversalSucceeded(Pair<Inode, Integer> inodeTraversal) {
    return inodeTraversal.getSecond() == -1;
  }

  // TODO: improve inode traversing
  private Pair<Inode, Integer> traverseToInode(String[] pathComponents)
      throws InvalidPathException {
    if (pathComponents == null || pathComponents.length == 0) {
      throw new InvalidPathException("passed-in pathComponents is null or empty");
    }
    if (pathComponents.length == 1) {
      if (pathComponents[0].equals("")) {
        return new Pair<Inode, Integer>(mRoot, -1);
      } else {
        final String msg = "File name starts with " + pathComponents[0];
        LOG.info("InvalidPathException: " + msg);
        throw new InvalidPathException(msg);
      }
    }

    Pair<Inode, Integer> ret = new Pair<Inode, Integer>(mRoot, -1);

    for (int k = 1; k < pathComponents.length; k ++) {
      Inode next = ((InodeDirectory) ret.getFirst()).getChild(pathComponents[k]);
      if (next == null) {
        // The user might want to create the nonexistent directories, so we leave ret.getFirst()
        // as the last Inode taken. We set nonexistentInd to k, to indicate that the kth path
        // component was the first one that couldn't be found.
        ret.setSecond(k);
        break;
      }
      ret.setFirst(next);
      if (!ret.getFirst().isDirectory()) {
        // The inode can't have any children. If this is the last path component, we're good.
        // Otherwise, we can't traverse further, so we clean up and throw an exception.
        if (k == pathComponents.length - 1) {
          break;
        } else {
          final String msg =
              "Traversal failed. Component " + k + "(" + ret.getFirst().getName() + ") is a file";
          LOG.info("InvalidPathException: " + msg);
          throw new InvalidPathException(msg);
        }
      }
    }
    return ret;
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
