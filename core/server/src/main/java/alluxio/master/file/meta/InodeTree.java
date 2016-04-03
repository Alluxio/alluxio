/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.collections.IndexedSet;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.master.MasterContext;
import alluxio.master.block.ContainerIdGenerable;
import alluxio.master.file.PermissionChecker;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.journal.JournalCheckpointStreamable;
import alluxio.master.journal.JournalOutputStream;
import alluxio.master.journal.JournalProtoUtils;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.PermissionStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.FormatUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents the tree of Inode's.
 */
@NotThreadSafe
// TODO(jiri): Make this class thread-safe.
public final class InodeTree implements JournalCheckpointStreamable {
  /** Value to be used for an inode with no parent. */
  public static final long NO_PARENT = -1;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Only the root inode should have the empty string as its name. */
  private static final String ROOT_INODE_NAME = "";

  /** The root of the entire file system. */
  private InodeDirectory mRoot = null;

  /** Mount table manages the file system mount points. */
  private MountTable mMountTable;

  private final IndexedSet.FieldIndex<Inode<?>> mIdIndex = new IndexedSet.FieldIndex<Inode<?>>() {
    @Override
    public Object getFieldValue(Inode<?> o) {
      return o.getId();
    }
  };
  @SuppressWarnings("unchecked")
  private final IndexedSet<Inode<?>> mInodes = new IndexedSet<Inode<?>>(mIdIndex);
  /** A set of inode ids representing pinned inode files. */
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
   * @param mountTable the mount table to manage the file system mount points
   */
  public InodeTree(ContainerIdGenerable containerIdGenerator,
      InodeDirectoryIdGenerator directoryIdGenerator, MountTable mountTable) {
    mContainerIdGenerator = containerIdGenerator;
    mDirectoryIdGenerator = directoryIdGenerator;
    mMountTable = mountTable;
  }

  /**
   * Initializes the root of the inode tree.
   *
   * @param rootPermissionStatus the root {@link PermissionStatus}
   */
  public void initializeRoot(PermissionStatus rootPermissionStatus) {
    if (mRoot == null) {
      mRoot = new InodeDirectory(mDirectoryIdGenerator.getNewDirectoryId()).setName(ROOT_INODE_NAME)
          .setPermissionStatus(rootPermissionStatus).setParentId(NO_PARENT);
      mInodes.add(mRoot);
      mCachedInode = mRoot;
    }
    PermissionChecker.initializeFileSystem(
        MasterContext.getConf().getBoolean(Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED),
        mRoot.getUserName(),
        MasterContext.getConf().get(Constants.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP));
  }

  /**
   * @return the number of total inodes
   */
  public int getSize() {
    return mInodes.size();
  }

  /**
   * @return the number of pinned inodes
   */
  public int getPinnedSize() {
    return mPinnedInodeFileIds.size();
  }

  /**
   * @param id the id to get the inode for
   * @return whether the inode exists
   */
  public boolean inodeIdExists(long id) {
    return mInodes.getFirstByField(mIdIndex, id) != null;
  }

  /**
   * @param path the path to get the inode for
   * @return whether the inode exists
   */
  public boolean inodePathExists(AlluxioURI path) {
    try {
      TraversalResult traversalResult =
          traverseToInode(PathUtils.getPathComponents(path.toString()), false);
      return traversalResult.isFound();
    } catch (InvalidPathException e) {
      return false;
    }
  }

  /**
   * @param id the id to get the inode for
   * @return the inode with the given id
   * @throws FileDoesNotExistException if the file does not exist
   */
  public Inode<?> getInodeById(long id) throws FileDoesNotExistException {
    Inode<?> inode = mInodes.getFirstByField(mIdIndex, id);
    if (inode == null) {
      throw new FileDoesNotExistException("Inode id " + id + " does not exist.");
    }
    return inode;
  }

  /**
   * @param path the path to get the inode for
   * @return the inode with the given path
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the path does not exist
   */
  public Inode<?> getInodeByPath(AlluxioURI path)
      throws InvalidPathException, FileDoesNotExistException {
    TraversalResult traversalResult =
        traverseToInode(PathUtils.getPathComponents(path.toString()), false);
    if (!traversalResult.isFound()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
    return traversalResult.getInode();
  }

  /**
   * @param path the path to get the inodes list for
   * @return the inodes list with the given path
   * @throws InvalidPathException if the path is invalid
   */
  public List<Inode<?>> collectInodes(AlluxioURI path) throws InvalidPathException {
    TraversalResult traversalResult =
        traverseToInode(PathUtils.getPathComponents(path.getPath()), false);
    return traversalResult.getInodes();
  }

  /**
   * @param inode the inode to get the path for
   * @return the path for a given inode
   */
  public AlluxioURI getPath(Inode<?> inode) {
    if (isRootId(inode.getId())) {
      return new AlluxioURI(AlluxioURI.SEPARATOR);
    }
    if (isRootId(inode.getParentId())) {
      return new AlluxioURI(AlluxioURI.SEPARATOR + inode.getName());
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
   * @throws IOException if creating the path fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  public CreatePathResult createPath(AlluxioURI path, CreatePathOptions<?> options)
      throws FileAlreadyExistsException, BlockInfoException, InvalidPathException, IOException,
      FileDoesNotExistException {
    if (path.isRoot()) {
      LOG.info(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path));
      throw new FileAlreadyExistsException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path));
    }
    if (options instanceof CreateFileOptions) {
      CreateFileOptions fileOptions = (CreateFileOptions) options;
      if (fileOptions.getBlockSizeBytes() < 1) {
        throw new BlockInfoException("Invalid block size " + fileOptions.getBlockSizeBytes());
      }
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
        final String msg = new StringBuilder().append("File ").append(path)
            .append(" creation failed. Component ")
            .append(traversalResult.getNonexistentPathIndex()).append("(")
            .append(parentPath[traversalResult.getNonexistentPathIndex()])
            .append(") does not exist").toString();
        LOG.info("FileDoesNotExistException: {}", msg);
        throw new FileDoesNotExistException(msg);
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
    List<Inode<?>> createdInodes = Lists.newArrayList();
    List<Inode<?>> modifiedInodes = Lists.newArrayList();
    // Directory persistence will not happen until the end of this method.
    List<Inode<?>> toPersistDirectories = Lists.newArrayList(traversalResult.getNonPersisted());
    if (pathIndex < parentPath.length || currentInodeDirectory.getChild(name) == null) {
      // (1) There are components in parent paths that need to be created. Or
      // (2) The last component of the path needs to be created.
      // In these two cases, the last traversed Inode will be modified.
      modifiedInodes.add(currentInodeDirectory);
    }

    // Fill in the directories that were missing.
    for (int k = pathIndex; k < parentPath.length; k++) {
      Inode<?> dir =
          new InodeDirectory(mDirectoryIdGenerator.getNewDirectoryId()).setName(pathComponents[k])
              .setParentId(currentInodeDirectory.getId()).setPersistenceState(
              options.isPersisted() ? PersistenceState.PERSISTED : PersistenceState.NOT_PERSISTED)
              .setPermissionStatus(options.getPermissionStatus());
      dir.setPinned(currentInodeDirectory.isPinned());
      currentInodeDirectory.addChild(dir);
      currentInodeDirectory.setLastModificationTimeMs(options.getOperationTimeMs());
      if (options.isPersisted()) {
        toPersistDirectories.add(dir);
      }
      createdInodes.add(dir);
      mInodes.add(dir);
      currentInodeDirectory = (InodeDirectory) dir;
    }

    // Create the final path component. First we need to make sure that there isn't already a file
    // here with that name. If there is an existing file that is a directory and we're creating a
    // directory, update persistence property of the directories if needed, otherwise, throw
    // FileAlreadyExistsException unless options.allowExists is true.
    Inode<?> lastInode = currentInodeDirectory.getChild(name);
    if (lastInode != null) {
      if (lastInode.isDirectory() && options instanceof CreateDirectoryOptions && !lastInode
          .isPersisted() && options.isPersisted()) {
        // The final path component already exists and is not persisted, so it should be added
        // to the non-persisted Inodes of traversalResult.
        traversalResult.getNonPersisted().add(lastInode);
        toPersistDirectories.add(lastInode);
      } else if (!lastInode.isDirectory() || !(options instanceof CreateDirectoryOptions
          && ((CreateDirectoryOptions) options).isAllowExists())) {
        LOG.info(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path));
        throw new FileAlreadyExistsException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path));
      }
    } else {
      if (options instanceof CreateDirectoryOptions) {
        CreateDirectoryOptions directoryOptions = (CreateDirectoryOptions) options;
        lastInode = new InodeDirectory(mDirectoryIdGenerator.getNewDirectoryId()).setName(name)
            .setParentId(currentInodeDirectory.getId())
            .setPermissionStatus(directoryOptions.getPermissionStatus())
            .setMountPoint(directoryOptions.isMountPoint());
        if (directoryOptions.isPersisted()) {
          toPersistDirectories.add(lastInode);
        }
      }
      if (options instanceof CreateFileOptions) {
        CreateFileOptions fileOptions = (CreateFileOptions) options;
        lastInode = new InodeFile(mContainerIdGenerator.getNewContainerId())
            .setBlockSizeBytes(fileOptions.getBlockSizeBytes()).setTtl(fileOptions.getTtl())
            .setName(name).setParentId(currentInodeDirectory.getId()).setPersistenceState(
                fileOptions.isPersisted() ? PersistenceState.PERSISTED :
                    PersistenceState.NOT_PERSISTED)
            .setPermissionStatus(fileOptions.getPermissionStatus());
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
    }

    if (toPersistDirectories.size() > 0) {
      Inode<?> lastToPersistInode = toPersistDirectories.get(toPersistDirectories.size() - 1);
      String ufsPath = mMountTable.resolve(getPath(lastToPersistInode)).toString();
      UnderFileSystem ufs = UnderFileSystem.get(ufsPath, MasterContext.getConf());
      // Persists only the last directory, recursively creating necessary parent directories. Even
      // if the directory already exists in the ufs, we mark it as persisted.
      if (ufs.exists(ufsPath) || ufs.mkdirs(ufsPath, true)) {
        for (Inode<?> inode : toPersistDirectories) {
          inode.setPersistenceState(PersistenceState.PERSISTED);
        }
      }
    }

    LOG.debug("createFile: File Created: {} parent: ", lastInode, currentInodeDirectory);
    return new CreatePathResult(modifiedInodes, createdInodes, traversalResult.getNonPersisted());
  }

  /**
   * Reinitializes the block size and TTL of an existing open file.
   *
   * @param path the path to the file
   * @param blockSizeBytes the new block size
   * @param ttl the ttl
   * @return the file id
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the path does not exist
   */
  public long reinitializeFile(AlluxioURI path, long blockSizeBytes, long ttl)
      throws InvalidPathException, FileDoesNotExistException {
    InodeFile file = (InodeFile) getInodeByPath(path);
    file.setBlockSizeBytes(blockSizeBytes);
    file.setTtl(ttl);
    return file.getId();
  }

  /**
   * Returns a list of all descendants of a particular {@link InodeDirectory}. Any directory inode
   * precedes its descendants in the list.
   *
   * @param inodeDirectory the root {@link InodeDirectory} to retrieve all descendants from
   * @return a list of all descendants
   */
  public List<Inode<?>> getInodeChildrenRecursive(InodeDirectory inodeDirectory) {
    List<Inode<?>> ret = new ArrayList<Inode<?>>();
    for (Inode<?> i : inodeDirectory.getChildren()) {
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
   * @throws FileDoesNotExistException if the Inode cannot be retrieved
   */
  public void deleteInode(Inode<?> inode, long opTimeMs) throws FileDoesNotExistException {
    InodeDirectory parent = (InodeDirectory) getInodeById(inode.getParentId());
    parent.removeChild(inode);
    parent.setLastModificationTimeMs(opTimeMs);

    mInodes.remove(inode);
    mPinnedInodeFileIds.remove(inode.getId());
    inode.setDeleted(true);
  }

  /**
   * Deletes a single inode from the inode tree by removing it from the parent inode.
   *
   * @param inode The {@link Inode} to delete
   * @throws FileDoesNotExistException if the Inode cannot be retrieved
   */
  public void deleteInode(Inode<?> inode) throws FileDoesNotExistException {
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
  public void setPinned(Inode<?> inode, boolean pinned, long opTimeMs) {
    inode.setPinned(pinned);
    inode.setLastModificationTimeMs(opTimeMs);

    if (inode.isFile()) {
      if (inode.isPinned()) {
        mPinnedInodeFileIds.add(inode.getId());
      } else {
        mPinnedInodeFileIds.remove(inode.getId());
      }
    } else {
      assert inode instanceof InodeDirectory;
      // inode is a directory. Set the pinned state for all children.
      for (Inode<?> child : ((InodeDirectory) inode).getChildren()) {
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
  public void setPinned(Inode<?> inode, boolean pinned) {
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
    Preconditions.checkNotNull(mRoot, PreconditionMessage.INODE_TREE_UNINITIALIZED_IS_ROOT_ID);
    return fileId == mRoot.getId();
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    // Write tree via breadth-first traversal, so that during deserialization, it may be more
    // efficient than depth-first during deserialization due to parent directory's locality.
    Queue<Inode<?>> inodes = new LinkedList<Inode<?>>();
    inodes.add(mRoot);
    while (!inodes.isEmpty()) {
      Inode<?> inode = inodes.poll();
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
   * @throws AccessControlException when owner of mRoot is not the owner of root journal entry
   */
  public void addInodeFromJournal(JournalEntry entry) throws AccessControlException {
    Message innerEntry = JournalProtoUtils.unwrap(entry);
    if (innerEntry instanceof InodeFileEntry) {
      InodeFile file = InodeFile.fromJournalEntry((InodeFileEntry) innerEntry);
      addInodeFromJournalInternal(file);
    } else if (innerEntry instanceof InodeDirectoryEntry) {
      InodeDirectory directory = InodeDirectory.fromJournalEntry((InodeDirectoryEntry) innerEntry);

      if (directory.getName().equals(ROOT_INODE_NAME)) {
        // This is the root inode. Clear all the state, and set the root.
        if (SecurityUtils.isSecurityEnabled(MasterContext.getConf())
            && mRoot != null && !mRoot.getUserName().equals(directory.getUserName())) {
          // user is not the owner of journal root entry
          throw new AccessControlException(
              ExceptionMessage.PERMISSION_DENIED.getMessage("Unauthorized user on root"));
        }
        mInodes.clear();
        mPinnedInodeFileIds.clear();
        mRoot = directory;
        mCachedInode = mRoot;
        mInodes.add(mRoot);
      } else {
        addInodeFromJournalInternal(directory);
      }
    } else {
      LOG.error("Unexpected InodeEntry journal entry: {}", entry);
    }
  }

  /**
   * Adds a given inode into the inode tree, by adding the inode to its parent. Also updates the
   * appropriate inode indexes.
   *
   * @param inode the inode to add to the inode tree
   */
  private void addInodeFromJournalInternal(Inode<?> inode) {
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InodeTree)) {
      return false;
    }
    InodeTree that = (InodeTree) o;
    return Objects.equal(mRoot, that.mRoot)
        && Objects.equal(mIdIndex, that.mIdIndex)
        && Objects.equal(mInodes, that.mInodes)
        && Objects.equal(mPinnedInodeFileIds, that.mPinnedInodeFileIds)
        && Objects.equal(mContainerIdGenerator, that.mContainerIdGenerator)
        && Objects.equal(mDirectoryIdGenerator, that.mDirectoryIdGenerator)
        && Objects.equal(mCachedInode, that.mCachedInode);
  }

  private TraversalResult traverseToInode(String[] pathComponents, boolean collectNonPersisted)
      throws InvalidPathException {
    List<Inode<?>> nonPersistedInodes = Lists.newArrayList();
    List<Inode<?>> inodes = Lists.newArrayList();

    if (pathComponents == null) {
      throw new InvalidPathException("passed-in pathComponents is null");
    } else if (pathComponents.length == 0) {
      throw new InvalidPathException("passed-in pathComponents is empty");
    } else if (pathComponents.length == 1) {
      if (pathComponents[0].equals("")) {
        inodes.add(mRoot);
        return TraversalResult.createFoundResult(mRoot, nonPersistedInodes, inodes);
      } else {
        throw new InvalidPathException("File name starts with " + pathComponents[0]);
      }
    }

    Inode<?> current = mRoot;
    inodes.add(current);

    // iterate from 1, because 0 is root and it's already added
    for (int i = 1; i < pathComponents.length; i++) {
      Inode<?> next = ((InodeDirectory) current).getChild(pathComponents[i]);
      if (next == null) {
        // The user might want to create the nonexistent directories, so return the traversal result
        // current inode with the last Inode taken, and the index of the first path component that
        // couldn't be found.
        return TraversalResult.createNotFoundResult(current, i, nonPersistedInodes, inodes);
      } else if (next.isFile()) {
        // The inode can't have any children. If this is the last path component, we're good.
        // Otherwise, we can't traverse further, so we clean up and throw an exception.
        if (i == pathComponents.length - 1) {
          inodes.add(next);
          return TraversalResult.createFoundResult(next, nonPersistedInodes, inodes);
        } else {
          throw new InvalidPathException(
              "Traversal failed. Component " + i + "(" + next.getName() + ") is a file");
        }
      } else {
        inodes.add(next);
        if (!next.isPersisted() && collectNonPersisted) {
          // next is a directory and not persisted
          nonPersistedInodes.add(next);
        }
        current = next;
      }
    }
    return TraversalResult.createFoundResult(current, nonPersistedInodes, inodes);
  }

  private static final class TraversalResult {
    private final boolean mFound;
    /**
     * When the path is not found in a traversal, the index of the first path component that
     * couldn't be found.
     */
    private final int mNonexistentIndex;
    /**
     * The found inode when the traversal succeeds; otherwise the last path component navigated.
     */
    private final Inode<?> mInode;

    /**
     * The list of non-persisted inodes encountered during the traversal.
     */
    private final List<Inode<?>> mNonPersisted;

    /**
     * The list of all inodes encountered during the traversal.
     */
    private final List<Inode<?>> mInodes;

    static TraversalResult createFoundResult(Inode<?> inode, List<Inode<?>> nonPersisted,
        List<Inode<?>> inodes) {
      return new TraversalResult(true, -1, inode, nonPersisted, inodes);
    }

    static TraversalResult createNotFoundResult(Inode<?> inode, int index,
        List<Inode<?>> nonPersisted, List<Inode<?>> inodes) {
      return new TraversalResult(false, index, inode, nonPersisted, inodes);
    }

    private TraversalResult(boolean found, int index, Inode<?> inode, List<Inode<?>> nonPersisted,
        List<Inode<?>> inodes) {
      mFound = found;
      mNonexistentIndex = index;
      mInode = inode;
      mNonPersisted = nonPersisted;
      mInodes = inodes;
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

    Inode<?> getInode() {
      return mInode;
    }

    /**
     * @return the list of non-persisted inodes encountered during the traversal
     */
    List<Inode<?>> getNonPersisted() {
      return mNonPersisted;
    }

    /**
     * @return the list of all inodes encountered during the traversal
     */
    List<Inode<?>> getInodes() {
      return mInodes;
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
    private final List<Inode<?>> mModified;
    private final List<Inode<?>> mCreated;
    private final List<Inode<?>> mPersisted;

    /**
     * Constructs the results of modified and created inodes when creating a path.
     *
     * @param modified a list of modified inodes
     * @param created a list of created inodes
     */
    CreatePathResult(List<Inode<?>> modified, List<Inode<?>> created, List<Inode<?>> persisted) {
      mModified = Preconditions.checkNotNull(modified);
      mCreated = Preconditions.checkNotNull(created);
      mPersisted = Preconditions.checkNotNull(persisted);
    }

    /**
     * @return the list of inodes modified during path creation
     */
    public List<Inode<?>> getModified() {
      return mModified;
    }

    /**
     * @return the list of inodes created during path creation
     */
    public List<Inode<?>> getCreated() {
      return mCreated;
    }

    /**
     * @return the list of existing inodes that were persisted during path creation
     */
    public List<Inode<?>> getPersisted() {
      return mPersisted;
    }
  }
}
