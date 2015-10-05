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

package tachyon.master.file.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.collections.IndexedSet;
import tachyon.exception.BlockInfoException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.master.block.ContainerIdGenerable;
import tachyon.master.file.journal.InodeDirectoryEntry;
import tachyon.master.file.journal.InodeEntry;
import tachyon.master.file.journal.InodeFileEntry;
import tachyon.master.journal.JournalCheckpointStreamable;
import tachyon.master.journal.JournalOutputStream;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;

public final class InodeTree implements JournalCheckpointStreamable {
  /** Value to be used for an inode with no parent. */
  public static final long NO_PARENT = -1;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Only the root inode should have the empty string as its name. */
  private static final String ROOT_INODE_NAME = "";

  /** The root of the entire file system. */
  private InodeDirectory mRoot;

  private final IndexedSet.FieldIndex<Inode> mIdIndex = new IndexedSet.FieldIndex<Inode>() {
    @Override
    public Object getFieldValue(Inode o) {
      return o.getId();
    }
  };
  @SuppressWarnings("unchecked")
  private final IndexedSet<Inode> mInodes = new IndexedSet<Inode>(mIdIndex);
  /** A set of inode ids representing pinned inode files */
  private final Set<Long> mPinnedInodeFileIds = new HashSet<Long>();

  /**
   * Inode id management. Inode ids are essentially block ids.
   *
   * inode files: Each file id will be composed of a unique block container id, with the maximum
   * sequence number.
   *
   * inode directories: Each directory id will be a unique block id, in order to avoid any collision
   * with file ids.
   */
  private final ContainerIdGenerable mContainerIdGenerator;
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;

  /**
   * This is only used for adding inodes from the journal, to prevent repeated lookups of the same
   * inode.
   */
  private InodeDirectory mCachedInode;

  /**
   * @param containerIdGenerator the container id generator to use to get new container ids.
   * @param directoryIdGenerator the directory id generator to use to get new directory ids.
   */
  public InodeTree(ContainerIdGenerable containerIdGenerator,
      InodeDirectoryIdGenerator directoryIdGenerator) {
    mContainerIdGenerator = containerIdGenerator;
    mDirectoryIdGenerator = directoryIdGenerator;
  }

  public void initializeRoot() {
    if (mRoot == null) {
      mRoot =
          new InodeDirectory(ROOT_INODE_NAME, mDirectoryIdGenerator.getNewDirectoryId(), NO_PARENT,
              System.currentTimeMillis());
      mInodes.add(mRoot);

      mCachedInode = mRoot;
    }
  }

  /**
   * Return the number of total inodes.
   *
   * @return the number of total inodes
   */
  public int getSize() {
    return mInodes.size();
  }

  /**
   * Return the number of pinned inodes.
   *
   * @return the number of pinned inodes
   */
  public int getPinnedSize() {
    return mPinnedInodeFileIds.size();
  }

  /**
   * @param id the id to get the inode for
   * @return the inode with the given id
   * @throws FileDoesNotExistException if the file does not exist
   */
  public Inode getInodeById(long id) throws FileDoesNotExistException {
    Inode inode = mInodes.getFirstByField(mIdIndex, id);
    if (inode == null) {
      throw new FileDoesNotExistException("Inode id " + id + " does not exist.");
    }
    return inode;
  }

  /**
   * @param path the path to get the inode for
   * @return the inode with the given path
   * @throws InvalidPathException if the path is invalid
   */
  public Inode getInodeByPath(TachyonURI path) throws InvalidPathException {
    TraversalResult traversalResult = traverseToInode(PathUtils.getPathComponents(path.toString()));
    if (!traversalResult.isFound()) {
      throw new InvalidPathException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
    return traversalResult.getInode();
  }

  /**
   * @param inode the inode to get the path for
   * @return the path for a given inode
   */
  public TachyonURI getPath(Inode inode) {
    if (isRootId(inode.getId())) {
      return new TachyonURI(TachyonURI.SEPARATOR);
    }
    if (isRootId(inode.getParentId())) {
      return new TachyonURI(TachyonURI.SEPARATOR + inode.getName());
    }
    return getPath(mInodes.getFirstByField(mIdIndex, inode.getParentId())).join(inode.getName());
  }

  /**
   * @return the root inode
   */
  public InodeDirectory getRoot() {
    return mRoot;
  }

  /**
   * Creates a file or directory at the given path.
   *
   * @param path the path
   * @param blockSizeBytes block size in bytes, if it is to create a file, the blockSizeBytes should
   *        not be fewer than 1, otherwise, it is ignored, can be set to 0
   * @param recursive if it is true, create any necessary but nonexistent parent directories of the
   *        path, otherwise, throw InvalidPathException if there some necessary parent directories
   *        is nonexistent
   * @param directory if it is true, create a directory, otherwise, create a file
   * @return a {@link CreatePathResult} representing the modified inodes and created inodes during
   *         path creation.
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws BlockInfoException if the block size is invalid
   * @throws InvalidPathException if the path is invalid
   */
  public CreatePathResult createPath(TachyonURI path, long blockSizeBytes, boolean recursive,
      boolean directory)
          throws FileAlreadyExistsException, BlockInfoException, InvalidPathException {
    return createPath(path, blockSizeBytes, recursive, directory, System.currentTimeMillis(),
        Constants.NO_TTL);
  }

  /**
   * Creates a file or directory at the given path.
   *
   * @param path the path
   * @param blockSizeBytes block size in bytes, if it is to create a file, the blockSizeBytes should
   *        not be fewer than 1, otherwise, it is ignored, can be set to 0
   * @param recursive if it is true, create any necessary but nonexistent parent directories of the
   *        path, otherwise, throw InvalidPathException if there some necessary parent directories
   *        is nonexistent
   * @param ttl ttl for file expiration
   * @param directory if it is true, create a directory, otherwise, create a file
   * @return a {@link CreatePathResult} representing the modified inodes and created inodes during
   *         path creation.
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws BlockInfoException if the block size is invalid
   * @throws InvalidPathException if the path is invalid
   */
  public CreatePathResult createPath(TachyonURI path, long blockSizeBytes, boolean recursive,
      boolean directory, long ttl)
          throws FileAlreadyExistsException, BlockInfoException, InvalidPathException {
    return createPath(path, blockSizeBytes, recursive, directory, System.currentTimeMillis(), ttl);
  }

  /**
   * Creates a file or directory at path.
   *
   * @param path the path
   * @param blockSizeBytes block size in bytes, if it is to create a file, the blockSizeBytes should
   *        not be fewer than 1, otherwise, it is ignored, can be set to 0
   * @param recursive if it is true, create any necessary but nonexistent parent directories of the
   *        path, otherwise, throw InvalidPathException if there some necessary parent directories
   *        is nonexistent
   * @param directory if it is true, create a directory, otherwise, create a file
   * @param creationTimeMs the time to create the inode
   * @param ttl time to live for file expiration
   * @return a {@link CreatePathResult} representing the modified inodes and created inodes during
   *         path creation.
   * @throws FileAlreadyExistsException when there is already a file at path if we want to create a
   *         directory there
   * @throws BlockInfoException when blockSizeBytes is invalid
   * @throws InvalidPathException when path is invalid, for example, (1) when there is nonexistent
   *         necessary parent directories and recursive is false, (2) when one of the necessary
   *         parent directories is actually a file
   */
  public CreatePathResult createPath(TachyonURI path, long blockSizeBytes, boolean recursive,
      boolean directory, long creationTimeMs, long ttl)
          throws FileAlreadyExistsException, BlockInfoException, InvalidPathException {
    if (path.isRoot()) {
      LOG.info("FileAlreadyExistsException: " + path);
      throw new FileAlreadyExistsException(path.toString());
    }
    if (!directory && blockSizeBytes < 1) {
      throw new BlockInfoException("Invalid block size " + blockSizeBytes);
    }

    LOG.debug("createPath {}", FormatUtils.parametersToString(path));

    String[] pathComponents = PathUtils.getPathComponents(path.toString());
    String name = path.getName();

    String[] parentPath = new String[pathComponents.length - 1];
    System.arraycopy(pathComponents, 0, parentPath, 0, parentPath.length);

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
    List<Inode> modifiedInodes = Lists.newArrayList();
    List<Inode> createdInodes = Lists.newArrayList();
    // Add the last found inode, since the modification time will be updated when descendants are
    // created.
    modifiedInodes.add(currentInodeDirectory);
    // Fill in the directories that were missing.
    for (int k = pathIndex; k < parentPath.length; k ++) {
      Inode dir = new InodeDirectory(pathComponents[k], mDirectoryIdGenerator.getNewDirectoryId(),
          currentInodeDirectory.getId(), creationTimeMs);
      dir.setPinned(currentInodeDirectory.isPinned());
      currentInodeDirectory.addChild(dir);
      currentInodeDirectory.setLastModificationTimeMs(creationTimeMs);
      createdInodes.add(dir);
      mInodes.add(dir);
      currentInodeDirectory = (InodeDirectory) dir;
    }

    // Create the final path component. First we need to make sure that there isn't already a file
    // here with that name. If there is an existing file that is a directory and we're creating a
    // directory, nothing needs to be done.
    Inode lastInode = currentInodeDirectory.getChild(name);
    if (lastInode != null) {
      if (lastInode.isDirectory() && directory) {
        return new CreatePathResult();
      }
      LOG.info("FileAlreadyExistsException: " + path);
      throw new FileAlreadyExistsException(path.toString());
    }
    if (directory) {
      lastInode = new InodeDirectory(name, mDirectoryIdGenerator.getNewDirectoryId(),
          currentInodeDirectory.getId(), creationTimeMs);
    } else {
      lastInode = new InodeFile(name, mContainerIdGenerator.getNewContainerId(),
          currentInodeDirectory.getId(), blockSizeBytes, creationTimeMs, ttl);
      if (currentInodeDirectory.isPinned()) {
        // Update set of pinned file ids.
        mPinnedInodeFileIds.add(lastInode.getId());
      }
    }
    lastInode.setPinned(currentInodeDirectory.isPinned());

    createdInodes.add(lastInode);
    mInodes.add(lastInode);
    currentInodeDirectory.addChild(lastInode);
    currentInodeDirectory.setLastModificationTimeMs(creationTimeMs);

    LOG.debug("createFile: File Created: {} parent: ", lastInode, currentInodeDirectory);
    return new CreatePathResult(modifiedInodes, createdInodes);
  }

  /**
   * Reinitializes the block size and TTL of an existing open file.
   *
   * @param path the path to the file
   * @param blockSizeBytes the new block size
   * @param ttl the ttl
   * @return the file id
   * @throws InvalidPathException if the paht is invalid
   */
  public long reinitializeFile(TachyonURI path, long blockSizeBytes, long ttl)
      throws InvalidPathException {
    // TODO(yupeng): add validation
    InodeFile file = (InodeFile) getInodeByPath(path);
    file.setBlockSize(blockSizeBytes);
    file.setTTL(ttl);;
    return file.getId();
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
   * @param opTimeMs The operation time
   */
  public void deleteInode(Inode inode, long opTimeMs) throws FileDoesNotExistException {
    InodeDirectory parent = (InodeDirectory) getInodeById(inode.getParentId());
    parent.removeChild(inode);
    parent.setLastModificationTimeMs(opTimeMs);

    mInodes.remove(inode);
    mPinnedInodeFileIds.remove(inode.getId());
    inode.delete();
  }

  /**
   * Deletes a single inode from the inode tree by removing it from the parent inode.
   *
   * @param inode The {@link Inode} to delete
   */
  public void deleteInode(Inode inode) throws FileDoesNotExistException {
    deleteInode(inode, System.currentTimeMillis());
  }

  /**
   * Sets the pinned state of an inode. If the inode is a directory, the pinned state will be set
   * recursively.
   *
   * @param inode The {@link Inode} to set the pinned state for.
   * @param pinned The pinned state to set for the inode (and possible descendants).
   * @param opTimeMs The operation time
   */
  public void setPinned(Inode inode, boolean pinned, long opTimeMs) {
    inode.setPinned(pinned);
    inode.setLastModificationTimeMs(opTimeMs);

    if (inode.isFile()) {
      if (inode.isPinned()) {
        mPinnedInodeFileIds.add(inode.getId());
      } else {
        mPinnedInodeFileIds.remove(inode.getId());
      }
    } else {
      // inode is a directory. Set the pinned state for all children.
      for (Inode child : ((InodeDirectory) inode).getChildren()) {
        setPinned(child, pinned, opTimeMs);
      }
    }
  }

  /**
   * Sets the pinned state of an inode. If the inode is a directory, the pinned state will be set
   * recursively.
   *
   * @param inode The {@link Inode} to set the pinned state for.
   * @param pinned The pinned state to set for the inode (and possible descendants).
   */
  public void setPinned(Inode inode, boolean pinned) {
    setPinned(inode, pinned, System.currentTimeMillis());
  }

  /**
   * @return the set of file ids which are pinned.
   */
  public Set<Long> getPinIdSet() {
    return Sets.newHashSet(mPinnedInodeFileIds);
  }

  /**
   * @param fileId the file id to check
   * @return true if the given file id is the root id
   */
  public boolean isRootId(long fileId) {
    return fileId == mRoot.getId();
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    // Write tree via breadth-first traversal, so that during deserialization, it may be more
    // efficient than depth-first during deserialization due to parent directory's locality.
    Queue<Inode> inodes = new LinkedList<Inode>();
    inodes.add(mRoot);
    while (!inodes.isEmpty()) {
      Inode inode = inodes.poll();
      outputStream.writeEntry(inode.toJournalEntry());
      if (inode.isDirectory()) {
        inodes.addAll(((InodeDirectory) inode).getChildren());
      }
    }
  }

  /**
   * Adds the inode represented by the entry parameter into the inode tree. If the inode entry
   * represents the root inode, the tree is "reset", and all state is cleared.
   *
   * @param entry The journal entry representing an inode.
   */
  public void addInodeFromJournal(InodeEntry entry) {
    if (entry instanceof InodeFileEntry) {
      InodeFile file = ((InodeFileEntry) entry).toInodeFile();
      addInodeFromJournalInternal(file);
    } else if (entry instanceof InodeDirectoryEntry) {
      InodeDirectory directory = ((InodeDirectoryEntry) entry).toInodeDirectory();

      if (directory.getName() == ROOT_INODE_NAME) {
        // This is the root inode. Clear all the state, and set the root.
        mInodes.clear();
        mPinnedInodeFileIds.clear();
        mRoot = directory;
        mCachedInode = mRoot;
        mInodes.add(mRoot);
      } else {
        addInodeFromJournalInternal(directory);
      }
    } else {
      LOG.error("Unexpected InodeEntry journal entry: " + entry);
    }
  }

  /**
   * Adds a given inode into the inode tree, by adding the inode to its parent. Also updates the
   * appropriate inode indexes.
   *
   * @param inode the inode to add to the inode tree.
   */
  private void addInodeFromJournalInternal(Inode inode) {
    InodeDirectory parentDirectory = mCachedInode;
    if (inode.getParentId() != mCachedInode.getId()) {
      parentDirectory =
          (InodeDirectory) mInodes.getFirstByField(mIdIndex, inode.getParentId());
      mCachedInode = parentDirectory;
    }
    parentDirectory.addChild(inode);
    mInodes.add(inode);
    // Update indexes.
    if (inode.isFile() && inode.isPinned()) {
      mPinnedInodeFileIds.add(inode.getId());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRoot, mIdIndex, mInodes, mPinnedInodeFileIds, mContainerIdGenerator,
        mDirectoryIdGenerator, mCachedInode);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof InodeTree) {
      InodeTree that = (InodeTree) object;
      return Objects.equal(mRoot, that.mRoot) && Objects.equal(mIdIndex, that.mIdIndex)
          && Objects.equal(mInodes, that.mInodes)
          && Objects.equal(mPinnedInodeFileIds, that.mPinnedInodeFileIds)
          && Objects.equal(mContainerIdGenerator, that.mContainerIdGenerator)
          && Objects.equal(mDirectoryIdGenerator, that.mDirectoryIdGenerator)
          && Objects.equal(mCachedInode, that.mCachedInode);
    }
    return false;
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
   * Represents the results of creating a path in the inode tree. This keeps track of inodes which
   * were modified, and inodes which were newly created during the path creation.
   */
  public static final class CreatePathResult {
    private final List<Inode> mModified;
    private final List<Inode> mCreated;

    /**
     * Constructs the results of modified and created inodes when creating a path.
     *
     * @param modified a list of modified inodes
     * @param created a list of created inodes
     */
    CreatePathResult(List<Inode> modified, List<Inode> created) {
      mModified = Preconditions.checkNotNull(modified);
      mCreated = Preconditions.checkNotNull(created);
    }

    /**
     * Constructs an empty result when creating a path.
     */
    CreatePathResult() {
      mModified = Collections.emptyList();
      mCreated = Collections.emptyList();
    }

    /**
     * @return the list of inodes modified during path creation.
     */
    public List<Inode> getModified() {
      return mModified;
    }

    /**
     * @return the list of inodes created during path creation.
     */
    public List<Inode> getCreated() {
      return mCreated;
    }
  }
}
