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
import tachyon.master.MasterContext;
import tachyon.master.block.ContainerIdGenerable;
import tachyon.master.file.journal.InodeDirectoryEntry;
import tachyon.master.file.journal.InodeEntry;
import tachyon.master.file.journal.InodeFileEntry;
import tachyon.master.file.meta.options.CreatePathOptions;
import tachyon.master.journal.JournalCheckpointStreamable;
import tachyon.master.journal.JournalOutputStream;
import tachyon.security.authorization.PermissionStatus;
import tachyon.underfs.UnderFileSystem;
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

  /** Mount table manages the file system mount points. */
  private MountTable mMountTable;

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
   * @param containerIdGenerator the container id generator to use to get new container ids
   * @param directoryIdGenerator the directory id generator to use to get new directory ids
   */
  public InodeTree(ContainerIdGenerable containerIdGenerator,
      InodeDirectoryIdGenerator directoryIdGenerator, MountTable mountTable) {
    mContainerIdGenerator = containerIdGenerator;
    mDirectoryIdGenerator = directoryIdGenerator;
    mMountTable = mountTable;
  }

  public void initializeRoot(PermissionStatus rootPS) {
    if (mRoot == null) {
      mRoot =
          new InodeDirectory.Builder().setName(ROOT_INODE_NAME)
              .setId(mDirectoryIdGenerator.getNewDirectoryId())
              .setPermissionStatus(rootPS).setParentId(NO_PARENT).build();
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
  public Inode getInodeByPath(TachyonURI path) throws InvalidPathException, IOException {
    TraversalResult traversalResult =
        traverseToInode(PathUtils.getPathComponents(path.toString()), false);
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
   * Creates a file or directory at path.
   *
   * @param path the path
   * @param options method options
   * @return a {@link CreatePathResult} representing the modified inodes and created inodes during
   *         path creation
   * @throws FileAlreadyExistsException when there is already a file at path if we want to create a
   *         directory there
   * @throws BlockInfoException when blockSizeBytes is invalid
   * @throws InvalidPathException when path is invalid, for example, (1) when there is nonexistent
   *         necessary parent directories and recursive is false, (2) when one of the necessary
   *         parent directories is actually a file
   */
  public CreatePathResult createPath(TachyonURI path, CreatePathOptions options)
      throws FileAlreadyExistsException, BlockInfoException, InvalidPathException, IOException {
    if (path.isRoot()) {
      LOG.info("FileAlreadyExistsException: " + path);
      throw new FileAlreadyExistsException(path.toString());
    }
    if (!options.isDirectory() && options.getBlockSizeBytes() < 1) {
      throw new BlockInfoException("Invalid block size " + options.getBlockSizeBytes());
    }

    LOG.debug("createPath {}", FormatUtils.parametersToString(path));

    String[] pathComponents = PathUtils.getPathComponents(path.getPath());
    String name = path.getName();

    String[] parentPath = new String[pathComponents.length - 1];
    System.arraycopy(pathComponents, 0, parentPath, 0, parentPath.length);

    TraversalResult traversalResult = traverseToInode(parentPath, options.isPersisted());
    // pathIndex is the index into pathComponents where we start filling in the path from the inode.
    int pathIndex = parentPath.length;
    if (!traversalResult.isFound()) {
      // Then the path component at errorInd k doesn't exist. If it's not recursive, we throw an
      // exception here. Otherwise we add the remaining path components to the list of components
      // to create.
      if (!options.isRecursive()) {
        final String msg = "File " + path + " creation failed. Component "
            + traversalResult.getNonexistentPathIndex() + "("
            + parentPath[traversalResult.getNonexistentPathIndex()] + ") does not exist";
        LOG.info("InvalidPathException: " + msg);
        throw new InvalidPathException(msg);
      } else {
        // We will start filling at the index of the non-existing step found by the traversal.
        pathIndex = traversalResult.getNonexistentPathIndex();
      }
    }

    if (!traversalResult.getInode().isDirectory()) {
      throw new InvalidPathException("Could not traverse to parent directory of path " + path
          + ". Component " + pathComponents[pathIndex - 1] + " is not a directory.");
    }
    InodeDirectory currentInodeDirectory = (InodeDirectory) traversalResult.getInode();
    List<Inode> createdInodes = Lists.newArrayList();
    List<Inode> modifiedInodes = Lists.newArrayList();
    if (pathIndex < parentPath.length || currentInodeDirectory.getChild(name) == null) {
      // (1) There are components in parent paths that need to be created. Or
      // (2) The last component of the path needs to be created.
      // In these two cases, the last traversed Inode will be modified.
      modifiedInodes.add(currentInodeDirectory);
    }

    // Fill in the directories that were missing.
    for (int k = pathIndex; k < parentPath.length; k ++) {
      Inode dir =
          new InodeDirectory.Builder().setName(pathComponents[k])
              .setId(mDirectoryIdGenerator.getNewDirectoryId())
              .setParentId(currentInodeDirectory.getId())
              .setPersisted(options.isPersisted())
              .setCreationTimeMs(options.getOperationTimeMs())
              .setPermissionStatus(options.getPermissionStatus())
              .build();
      dir.setPinned(currentInodeDirectory.isPinned());
      currentInodeDirectory.addChild(dir);
      currentInodeDirectory.setLastModificationTimeMs(options.getOperationTimeMs());
      if (options.isPersisted()) {
        String ufsPath = mMountTable.resolve(getPath(dir)).toString();
        UnderFileSystem ufs = UnderFileSystem.get(ufsPath, MasterContext.getConf());
        ufs.mkdirs(ufsPath, false);
      }
      createdInodes.add(dir);
      mInodes.add(dir);
      currentInodeDirectory = (InodeDirectory) dir;
    }

    // Create the final path component. First we need to make sure that there isn't already a file
    // here with that name. If there is an existing file that is a directory and we're creating a
    // directory, update persistence property of the directories if needed, otherwise, nothing needs
    // to be done.
    Inode lastInode = currentInodeDirectory.getChild(name);
    if (lastInode != null) {
      if (lastInode.isDirectory() && options.isDirectory()) {
        if (!lastInode.isPersisted() && options.isPersisted()) {
          lastInode.setPersisted(true);
          traversalResult.getPersisted().add(lastInode);
          String ufsPath = mMountTable.resolve(getPath(lastInode)).toString();
          UnderFileSystem ufs = UnderFileSystem.get(ufsPath, MasterContext.getConf());
          ufs.mkdirs(ufsPath, false);
        }
        return new CreatePathResult(modifiedInodes, createdInodes, traversalResult.getPersisted());
      }
      LOG.info("FileAlreadyExistsException: " + path);
      throw new FileAlreadyExistsException(path.toString());
    }
    if (options.isDirectory()) {
      lastInode =
          new InodeDirectory.Builder().setName(name)
              .setId(mDirectoryIdGenerator.getNewDirectoryId())
              .setParentId(currentInodeDirectory.getId()).setPersisted(options.isPersisted())
              .setPermissionStatus(options.getPermissionStatus())
              .build();
      if (options.isPersisted()) {
        String ufsPath = mMountTable.resolve(getPath(lastInode)).toString();
        UnderFileSystem ufs = UnderFileSystem.get(ufsPath, MasterContext.getConf());
        ufs.mkdirs(ufsPath, false);
      }
    } else {
      lastInode =
          new InodeFile.Builder().setBlockContainerId(mContainerIdGenerator.getNewContainerId())
              .setBlockSizeBytes(options.getBlockSizeBytes()).setTTL(options.getTTL()).setName(name)
              .setParentId(currentInodeDirectory.getId()).setPersisted(options.isPersisted())
              .setCreationTimeMs(options.getOperationTimeMs())
              .setPermissionStatus(options.getPermissionStatus())
              .build();
      if (currentInodeDirectory.isPinned()) {
        // Update set of pinned file ids.
        mPinnedInodeFileIds.add(lastInode.getId());
      }
    }
    lastInode.setPinned(currentInodeDirectory.isPinned());

    createdInodes.add(lastInode);
    mInodes.add(lastInode);
    currentInodeDirectory.addChild(lastInode);
    currentInodeDirectory.setLastModificationTimeMs(options.getOperationTimeMs());

    LOG.debug("createFile: File Created: {} parent: ", lastInode, currentInodeDirectory);
    return new CreatePathResult(modifiedInodes, createdInodes, traversalResult.getPersisted());
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
      throws InvalidPathException, IOException {
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
   * @param inodeDirectory the root {@link InodeDirectory} to retrieve all descendants from
   * @return A list of all descendants
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
   * @param inode the {@link Inode} to set the pinned state for
   * @param pinned the pinned state to set for the inode (and possible descendants)
   * @param opTimeMs the operation time
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
   * @param inode the {@link Inode} to set the pinned state for
   * @param pinned the pinned state to set for the inode (and possible descendants)
   */
  public void setPinned(Inode inode, boolean pinned) {
    setPinned(inode, pinned, System.currentTimeMillis());
  }

  /**
   * @return the set of file ids which are pinned
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
   * @param entry the journal entry representing an inode
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
   * @param inode the inode to add to the inode tree
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

  private TraversalResult traverseToInode(String[] pathComponents, boolean persist)
      throws InvalidPathException, IOException {
    List<Inode> persisted = Lists.newArrayList();

    if (pathComponents == null) {
      throw new InvalidPathException("passed-in pathComponents is null");
    } else if (pathComponents.length == 0) {
      throw new InvalidPathException("passed-in pathComponents is empty");
    } else if (pathComponents.length == 1) {
      if (pathComponents[0].equals("")) {
        return TraversalResult.createFoundResult(mRoot, persisted);
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
        return TraversalResult.createNotFoundResult(current, i, persisted);
      } else if (next.isFile()) {
        // The inode can't have any children. If this is the last path component, we're good.
        // Otherwise, we can't traverse further, so we clean up and throw an exception.
        if (i == pathComponents.length - 1) {
          return TraversalResult.createFoundResult(next, persisted);
        } else {
          throw new InvalidPathException(
              "Traversal failed. Component " + i + "(" + next.getName() + ") is a file");
        }
      } else {
        // next is a directory and keep navigating
        if (persist && !next.isPersisted()) {
          next.setPersisted(true);
          persisted.add(next);
          String ufsPath = mMountTable.resolve(getPath(next)).toString();
          UnderFileSystem ufs = UnderFileSystem.get(ufsPath, MasterContext.getConf());
          ufs.mkdirs(ufsPath, false);
        }
        current = next;
      }
    }
    return TraversalResult.createFoundResult(current, persisted);
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

    private final List<Inode> mPersisted;

    static TraversalResult createFoundResult(Inode inode, List<Inode> persisted) {
      return new TraversalResult(true, -1, inode, persisted);
    }

    static TraversalResult createNotFoundResult(Inode inode, int index, List<Inode> persisted) {
      return new TraversalResult(false, index, inode, persisted);
    }

    private TraversalResult(boolean found, int index, Inode inode, List<Inode> persisted) {
      mFound = found;
      mNonexistentIndex = index;
      mInode = inode;
      mPersisted = persisted;
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

    List<Inode> getPersisted() {
      return mPersisted;
    }
  }

  /**
   * Represents the results of creating a path in the inode tree. This keeps track of inodes which
   * were modified, and inodes which were newly created during the path creation.
   *
   * In particular, a {@link CreatePathResult} consists of an ordered list of modified inodes and an
   * ordered list of created inodes. Appending the latter to the former produces a list of inodes
   * starting with the root inode and ending in the inode corresponding to the created path.
   */
  public static final class CreatePathResult {
    private final List<Inode> mModified;
    private final List<Inode> mCreated;
    private final List<Inode> mPersisted;

    /**
     * Constructs the results of modified and created inodes when creating a path.
     *
     * @param modified a list of modified inodes
     * @param created a list of created inodes
     */
    CreatePathResult(List<Inode> modified, List<Inode> created, List<Inode> persisted) {
      mModified = Preconditions.checkNotNull(modified);
      mCreated = Preconditions.checkNotNull(created);
      mPersisted = Preconditions.checkNotNull(persisted);
    }

    /**
     * Constructs an empty result when creating a path.
     */
    CreatePathResult() {
      mModified = Collections.emptyList();
      mCreated = Collections.emptyList();
      mPersisted = Collections.emptyList();
    }

    /**
     * @return the list of inodes modified during path creation
     */
    public List<Inode> getModified() {
      return mModified;
    }

    /**
     * @return the list of inodes created during path creation
     */
    public List<Inode> getCreated() {
      return mCreated;
    }

    /**
     * @return the list of existing inodes that were persisted during path creation
     */
    public List<Inode> getPersisted() {
      return mPersisted;
    }
  }
}
