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
import alluxio.collections.ConcurrentHashSet;
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

  public enum LockMode {
    READ,
    WRITE;

    static boolean isRead(LockMode lockMode) {
      return lockMode == READ;
    }

    static boolean isWrite(LockMode lockMode) {
      return !isRead(lockMode);
    }
  }

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
  private final Set<Long> mPinnedInodeFileIds = new ConcurrentHashSet<>();

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
      mRoot = InodeDirectory
          .create(mDirectoryIdGenerator.getNewDirectoryId(), NO_PARENT, ROOT_INODE_NAME,
              CreateDirectoryOptions.defaults().setPermissionStatus(rootPermissionStatus));
      mInodes.add(mRoot);
      mCachedInode = mRoot;
    }
  }

  /**
   * @return username of root of inode tree, null if the inode tree is not initialized
   */
  public String getRootUserName() {
    if (mRoot == null) {
      return null;
    }
    return mRoot.getUserName();
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
          traverseToInode(PathUtils.getPathComponents(path.toString()), LockMode.READ);
      traversalResult.getInodeLockGroup().close();
      return traversalResult.isFound();
    } catch (InvalidPathException e) {
      return false;
    }
  }

  public InodePath lockInodePath(AlluxioURI path, LockMode lockMode) throws InvalidPathException {
    TraversalResult traversalResult =
        traverseToInode(PathUtils.getPathComponents(path.getPath()), lockMode);
    return new ExtensibleInodePath(path, traversalResult.getInodes(),
        traversalResult.getInodeLockGroup());
  }

  public InodePath lockFullInodePath(AlluxioURI path, LockMode lockMode)
      throws InvalidPathException, FileDoesNotExistException {
    TraversalResult traversalResult =
        traverseToInode(PathUtils.getPathComponents(path.getPath()), lockMode);
    if (!traversalResult.isFound()) {
      traversalResult.getInodeLockGroup().close();
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
    return new ExtensibleInodePath(path, traversalResult.getInodes(),
        traversalResult.getInodeLockGroup());
  }

  public InodePath lockFullInodePath(long id, LockMode lockMode) throws FileDoesNotExistException {
    for (;;) {
      Inode<?> inode = mInodes.getFirstByField(mIdIndex, id);
      if (inode == null) {
        throw new FileDoesNotExistException("Inode id " + id + " does not exist.");
      }
      // Compute the path given the target inode.
      StringBuilder builder = new StringBuilder();
      computePathForInode(inode, builder);
      AlluxioURI uri = new AlluxioURI(builder.toString());

      try (InodePath inodePath = lockFullInodePath(uri, lockMode)) {
        if (inodePath.getInode().getId() == id) {
          return inodePath;
        }
        // The path does not end up at the target inode id. Repeat the traversal.
      } catch (InvalidPathException e) {
        // ignore and repeat the loop
        LOG.warn("Inode lookup id {} computed path {} mismatch id. Repeating.", id, uri);
      }
    }
  }

  public void ensureFullInodePath(InodePath inodePath, LockMode lockMode)
      throws InvalidPathException, FileDoesNotExistException {
    TraversalResult traversalResult = traverseToInode(inodePath, lockMode);
    if (!traversalResult.isFound()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(inodePath.getUri()));
    }
  }

  private void computePathForInode(Inode<?> inode, StringBuilder builder)
      throws FileDoesNotExistException {
    inode.lockRead();
    long id = inode.getId();
    long parentId = inode.getParentId();
    String name = inode.getName();
    inode.unlockRead();

    if (isRootId(id)) {
      builder.append(AlluxioURI.SEPARATOR);
    } else if (isRootId(parentId)) {
      builder.append(AlluxioURI.SEPARATOR);
      builder.append(name);
    } else {
      Inode<?> parentInode = mInodes.getFirstByField(mIdIndex, parentId);
      if (parentInode == null) {
        throw new FileDoesNotExistException("Inode id " + parentId + " does not exist.");
      }

      computePathForInode(parentInode, builder);
      builder.append(AlluxioURI.SEPARATOR);
      builder.append(name);
    }
  }

  // inode must already be locked!
  public AlluxioURI getPath(Inode<?> inode) throws FileDoesNotExistException {
    StringBuilder builder = new StringBuilder();
    computePathForInode(inode, builder);
    return new AlluxioURI(builder.toString());
  }

  /**
   * Returns a list of existing inodes on the given path.
   *
   * @param path the path to get the inodes list for
   * @return the inodes list with the given path
   * @throws InvalidPathException if the path is invalid
   */
  public List<Inode<?>> collectInodes(AlluxioURI path) throws InvalidPathException {
    TraversalResult traversalResult =
        traverseToInode(PathUtils.getPathComponents(path.getPath()), LockMode.READ);
    return traversalResult.getInodes();
  }

  /**
   * @return the root inode
   */
  public InodeDirectory getRoot() {
    return mRoot;
  }

  // package private for testing.
  CreatePathResult createPath(AlluxioURI path, CreatePathOptions<?> options)
      throws FileAlreadyExistsException, BlockInfoException, InvalidPathException, IOException,
      FileDoesNotExistException {
    try (InodePath inodePath = lockInodePath(path, LockMode.WRITE)) {
      return createPath(inodePath, options);
    }
  }

  /**
   * Creates a file or directory at path.
   *
   * @param inodePath the path
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
  public CreatePathResult createPath(InodePath inodePath, CreatePathOptions<?> options)
      throws FileAlreadyExistsException, BlockInfoException, InvalidPathException, IOException,
      FileDoesNotExistException {
    AlluxioURI path = inodePath.getUri();
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

    if (!(inodePath instanceof ExtensibleInodePath)) {
      throw new InvalidPathException("Not an ExtensibleInodePath: " + inodePath.getUri());
    }

    LOG.debug("createPath {}", path);

    // TODO(gpang): lock the last inode(s) with write lock.
    TraversalResult traversalResult = traverseToInode(inodePath, LockMode.WRITE);
    InodeLockGroup lockGroup = traversalResult.getInodeLockGroup();

    ExtensibleInodePath extensibleInodePath = (ExtensibleInodePath) inodePath;
    String[] pathComponents = extensibleInodePath.getPathComponents();
    String name = path.getName();

    // pathIndex is the index into pathComponents where we start filling in the path from the inode.
    int pathIndex = extensibleInodePath.getInodes().size();
    if (pathIndex < pathComponents.length - 1) {
      // The immediate parent was not found. If it's not recursive, we throw an exception here.
      // Otherwise we add the remaining path components to the list of components to create.
      if (!options.isRecursive()) {
        final String msg = new StringBuilder().append("File ").append(path)
            .append(" creation failed. Component ")
            .append(pathIndex).append("(")
            .append(pathComponents[pathIndex])
            .append(") does not exist").toString();
        LOG.info("FileDoesNotExistException: {}", msg);
        throw new FileDoesNotExistException(msg);
      }
    }
    // The ancestor inode (parent or ancestor) of the target path.
    Inode<?> ancestorInode = extensibleInodePath.getAncestorInode();
    if (!ancestorInode.isDirectory()) {
      throw new InvalidPathException("Could not traverse to parent directory of path " + path
          + ". Component " + pathComponents[pathIndex - 1] + " is not a directory.");
    }
    InodeDirectory currentInodeDirectory = (InodeDirectory) ancestorInode;

    List<Inode<?>> createdInodes = Lists.newArrayList();
    List<Inode<?>> modifiedInodes = Lists.newArrayList();
    // These are inodes that already exist, that should be journaled as persisted.
    List<Inode<?>> existingNonPersisted = Lists.newArrayList();
    // These are inodes to mark as persisted at the end of this method.
    List<Inode<?>> toPersistDirectories = Lists.newArrayList();
    if (options.isPersisted()) {
      // Directory persistence will not happen until the end of this method.
      toPersistDirectories.addAll(traversalResult.getNonPersisted());
      existingNonPersisted.addAll(traversalResult.getNonPersisted());
    }
    if (pathIndex < (pathComponents.length - 1) || currentInodeDirectory.getChild(name) == null) {
      // (1) There are components in parent paths that need to be created. Or
      // (2) The last component of the path needs to be created.
      // In these two cases, the last traversed Inode will be modified.
      modifiedInodes.add(currentInodeDirectory);
    }

    // Fill in the ancestor directories that were missing.
    CreateDirectoryOptions missingDirOptions = CreateDirectoryOptions.defaults()
        .setMountPoint(false)
        .setPersisted(options.isPersisted())
        .setPermissionStatus(options.getPermissionStatus());
    for (int k = pathIndex; k < (pathComponents.length - 1); k++) {
      InodeDirectory dir =
          InodeDirectory.create(mDirectoryIdGenerator.getNewDirectoryId(),
              currentInodeDirectory.getId(), pathComponents[k], missingDirOptions);
      // Lock the newly created inode before subsequent operations, and add it to the lock group.
//        lockGroup.lockWrite(dir);

      dir.setPinned(currentInodeDirectory.isPinned());
      currentInodeDirectory.addChild(dir);
      currentInodeDirectory.setLastModificationTimeMs(options.getOperationTimeMs());
      if (options.isPersisted()) {
        toPersistDirectories.add(dir);
      }
      createdInodes.add(dir);
      mInodes.add(dir);
      currentInodeDirectory = dir;
    }

    // Create the final path component. First we need to make sure that there isn't already a file
    // here with that name. If there is an existing file that is a directory and we're creating a
    // directory, update persistence property of the directories if needed, otherwise, throw
    // FileAlreadyExistsException unless options.allowExists is true.
    Inode<?> lastInode = currentInodeDirectory.getChild(name);
    if (lastInode != null) {
      // Lock the last inode before subsequent operations, and add it to the lock group.
//        lockGroup.lockWrite(lastInode);

      if (lastInode.isDirectory() && options instanceof CreateDirectoryOptions && !lastInode
          .isPersisted() && options.isPersisted()) {
        // The final path component already exists and is not persisted, so it should be added
        // to the non-persisted Inodes of traversalResult.
        existingNonPersisted.add(lastInode);
        toPersistDirectories.add(lastInode);
      } else if (!lastInode.isDirectory() || !(options instanceof CreateDirectoryOptions
          && ((CreateDirectoryOptions) options).isAllowExists())) {
        LOG.info(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path));
        throw new FileAlreadyExistsException(
            ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path));
      }
    } else {
      if (options instanceof CreateDirectoryOptions) {
        CreateDirectoryOptions directoryOptions = (CreateDirectoryOptions) options;
        lastInode = InodeDirectory.create(mDirectoryIdGenerator.getNewDirectoryId(),
            currentInodeDirectory.getId(), name, directoryOptions);
        // Lock the created inode before subsequent operations, and add it to the lock group.
//          lockGroup.lockWrite(lastInode);
        if (directoryOptions.isPersisted()) {
          toPersistDirectories.add(lastInode);
        }
      }
      if (options instanceof CreateFileOptions) {
        CreateFileOptions fileOptions = (CreateFileOptions) options;
        lastInode = InodeFile.create(mContainerIdGenerator.getNewContainerId(),
            currentInodeDirectory.getId(), name, fileOptions);
        // Lock the created inode before subsequent operations, and add it to the lock group.
//          lockGroup.lockWrite(lastInode);
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
      MountTable.Resolution resolution = mMountTable.resolve(getPath(lastToPersistInode));
      String ufsUri = resolution.getUri().toString();
      UnderFileSystem ufs = resolution.getUfs();
      // Persists only the last directory, recursively creating necessary parent directories. Even
      // if the directory already exists in the ufs, we mark it as persisted.
      if (ufs.exists(ufsUri) || ufs.mkdirs(ufsUri, true)) {
        for (Inode<?> inode : toPersistDirectories) {
          inode.setPersistenceState(PersistenceState.PERSISTED);
        }
      }
    }

    LOG.debug("createFile: File Created: {} parent: ", lastInode, currentInodeDirectory);
    return new CreatePathResult(modifiedInodes, createdInodes, existingNonPersisted);
  }

  /**
   * Reinitializes the block size and TTL of an existing open file.
   *
   * @param inodePath the path to the file
   * @param blockSizeBytes the new block size
   * @param ttl the ttl
   * @return the file id
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the path does not exist
   */
  public long reinitializeFile(InodePath inodePath, long blockSizeBytes, long ttl)
      throws InvalidPathException, FileDoesNotExistException {
    InodeFile file = inodePath.getInodeFile();
    file.setBlockSizeBytes(blockSizeBytes);
    file.setTtl(ttl);
    return file.getId();
  }

  /**
   * Returns a list of all descendants of a particular {@link InodeDirectory}. Any directory inode
   * precedes its descendants in the list.
   *
   * @param inodePath the root {@link InodePath} to retrieve all descendants from
   * @param lockMode the lock type to use
   * @return an {@link InodeLockGroup} representing the list of all descendants
   * @throws FileDoesNotExistException if inode does not exist
   */
  public InodeLockGroup getInodeChildrenRecursive(InodePath inodePath, LockMode lockMode)
      throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
    InodeLockGroup inodeGroup = new InodeLockGroup();
    if (!inode.isDirectory()) {
      return inodeGroup;
    }
    return getInodeChildrenRecursiveInternal((InodeDirectory) inode, lockMode, inodeGroup);
  }

  public InodeLockGroup getInodeChildrenRecursiveInternal(InodeDirectory inodeDirectory,
      LockMode lockMode, InodeLockGroup inodeGroup) {
    for (Inode<?> child : inodeDirectory.getChildren()) {
      if (LockMode.isRead(lockMode)) {
        inodeGroup.lockRead(child);
      } else {
        inodeGroup.lockWrite(child);
      }
      if (child.isDirectory()) {
        getInodeChildrenRecursiveInternal((InodeDirectory) child, lockMode, inodeGroup);
      }
    }
    return inodeGroup;
  }

  /**
   * Deletes a single inode from the inode tree by removing it from the parent inode.
   *
   * @param inodePath The {@link InodePath} to delete
   * @param opTimeMs The operation time
   * @throws FileDoesNotExistException if the Inode cannot be retrieved
   */
  public void deleteInode(InodePath inodePath, long opTimeMs) throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
    InodeDirectory parent = (InodeDirectory) mInodes.getFirstByField(mIdIndex, inode.getParentId());
    if (parent == null) {
      throw new FileDoesNotExistException("Inode id " + inode.getParentId() + " does not exist.");
    }
    parent.removeChild(inode);
    parent.setLastModificationTimeMs(opTimeMs);

    mInodes.remove(inode);
    mPinnedInodeFileIds.remove(inode.getId());
    inode.setDeleted(true);
  }

  /**
   * Deletes a single inode from the inode tree by removing it from the parent inode.
   *
   * @param inodePath The {@link InodePath} to delete
   * @throws FileDoesNotExistException if the Inode cannot be retrieved
   */
  public void deleteInode(InodePath inodePath) throws FileDoesNotExistException {
    deleteInode(inodePath, System.currentTimeMillis());
  }

  /**
   * Sets the pinned state of an inode. If the inode is a directory, the pinned state will be set
   * recursively.
   *
   * @param inodePath the {@link InodePath} to set the pinned state for
   * @param pinned the pinned state to set for the inode (and possible descendants)
   * @param opTimeMs the operation time
   * @throws FileDoesNotExistException if inode does not exist
   */
  public void setPinned(InodePath inodePath, boolean pinned, long opTimeMs)
      throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
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
      TempInodePathWithDescendant tempInodePath = new TempInodePathWithDescendant(inodePath);
      for (Inode<?> child : ((InodeDirectory) inode).getChildren()) {
        child.lockWrite();
        try {
          tempInodePath.setDescendant(child, getPath(child));
          setPinned(tempInodePath, pinned, opTimeMs);
        } finally {
          child.unlockWrite();
        }
      }
    }
  }

  /**
   * Sets the pinned state of an inode. If the inode is a directory, the pinned state will be set
   * recursively.
   *
   * @param inodePath the {@link InodePath} to set the pinned state for
   * @param pinned the pinned state to set for the inode (and possible descendants)
   * @throws FileDoesNotExistException if inode does not exist
   */
  public void setPinned(InodePath inodePath, boolean pinned) throws FileDoesNotExistException {
    setPinned(inodePath, pinned, System.currentTimeMillis());
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

  private TraversalResult traverseToInode(String[] pathComponents, LockMode lockMode)
      throws InvalidPathException {
    List<Inode<?>> nonPersistedInodes = Lists.newArrayList();
    List<Inode<?>> inodes = Lists.newArrayList();
    InodeLockGroup lockGroup = new InodeLockGroup();

    // This must be set to true before returning a valid value, otherwise all the inodes will be
    // unlocked.
    boolean valid = false;
    try {
      if (pathComponents == null) {
        throw new InvalidPathException("passed-in pathComponents is null");
      } else if (pathComponents.length == 0) {
        throw new InvalidPathException("passed-in pathComponents is empty");
      } else if (pathComponents.length == 1) {
        if (pathComponents[0].equals("")) {
//          lockGroup.lockRead(mRoot);
          inodes.add(mRoot);
          valid = true;
          return TraversalResult.createFoundResult(nonPersistedInodes, inodes, lockGroup);
        } else {
          throw new InvalidPathException("File name starts with " + pathComponents[0]);
        }
      }

//      lockGroup.lockRead(mRoot);
      inodes.add(mRoot);
      TraversalResult result =
          traverseToInodeInternal(pathComponents, inodes, nonPersistedInodes, lockGroup, lockMode);
      valid = true;
      return result;
    } finally {
      if (!valid) {
        lockGroup.close();
      }
    }
  }

  private TraversalResult traverseToInode(InodePath inodePath, LockMode lockMode)
      throws InvalidPathException {
    // the inodePath is guaranteed to already include at least the root inode.
    if (!(inodePath instanceof ExtensibleInodePath)) {
      throw new InvalidPathException("Not an ExtensibleInodePath: " + inodePath.getUri());
    }
    ExtensibleInodePath extensibleInodePath = (ExtensibleInodePath) inodePath;
    List<Inode<?>> inodes = extensibleInodePath.getInodes();
    InodeLockGroup lockGroup = extensibleInodePath.getLockGroup();
    List<Inode<?>> nonPersistedInodes = Lists.newArrayList();
    for (Inode<?> inode : inodes) {
      if (!inode.isPersisted()) {
        nonPersistedInodes.add(inode);
      }
    }
    return traverseToInodeInternal(extensibleInodePath.getPathComponents(), inodes,
        nonPersistedInodes, lockGroup, lockMode);
  }

  // TODO(gpang): support locking inodes with write lock.
  private TraversalResult traverseToInodeInternal(String[] pathComponents, List<Inode<?>> inodes,
      List<Inode<?>> nonPersistedInodes, InodeLockGroup lockGroup,
      LockMode lockMode) throws InvalidPathException {
    // This must be set to true when returning a valid response. Otherwise, all the locked inodes
    // will be unlocked.
    boolean valid = false;
    try {
      Inode<?> current = inodes.get(inodes.size() - 1);

      for (int i = inodes.size(); i < pathComponents.length; i++) {
        Inode<?> next = ((InodeDirectory) current).getChild(pathComponents[i]);
        if (next == null) {
          // The user might want to create the nonexistent directories, so return the traversal
          // result current inode with the last Inode taken, and the index of the first path
          // component that couldn't be found.
          valid = true;
          return TraversalResult.createNotFoundResult(i, nonPersistedInodes, inodes, lockGroup);
        } else {
          // Lock the existing inode before proceeding.
//          lockGroup.lockRead(next);
          if (next.isFile()) {
            // The inode can't have any children. If this is the last path component, we're good.
            // Otherwise, we can't traverse further, so we clean up and throw an exception.
            if (i == pathComponents.length - 1) {
              inodes.add(next);
              valid = true;
              return TraversalResult.createFoundResult(nonPersistedInodes, inodes, lockGroup);
            } else {
              throw new InvalidPathException(
                  "Traversal failed. Component " + i + "(" + next.getName() + ") is a file");
            }
          } else {
            inodes.add(next);
            if (!next.isPersisted()) {
              // next is a directory and not persisted
              nonPersistedInodes.add(next);
            }
            current = next;
          }
        }
      }
      valid = true;
      return TraversalResult.createFoundResult(nonPersistedInodes, inodes, lockGroup);
    } finally {
      if (!valid) {
        lockGroup.close();
      }
    }
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

    /** The {@link InodeLockGroup} managing the locks for the inodes. */
    private final InodeLockGroup mLockGroup;

    // TODO(gpang): consider a builder paradigm to iteratively build the traversal result.
    static TraversalResult createFoundResult(List<Inode<?>> nonPersisted, List<Inode<?>> inodes,
        InodeLockGroup lockGroup) {
      return new TraversalResult(true, -1, nonPersisted, inodes, lockGroup);
    }

    static TraversalResult createNotFoundResult(int index, List<Inode<?>> nonPersisted,
        List<Inode<?>> inodes, InodeLockGroup lockGroup) {
      return new TraversalResult(false, index, nonPersisted, inodes, lockGroup);
    }

    private TraversalResult(boolean found, int index, List<Inode<?>> nonPersisted,
        List<Inode<?>> inodes, InodeLockGroup lockGroup) {
      mFound = found;
      mNonexistentIndex = index;
      mInode = inodes.get(inodes.size() - 1);
      mNonPersisted = nonPersisted;
      mInodes = inodes;
      mLockGroup = lockGroup;
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

    /**
     * @return the {@link InodeLockGroup} managing the locks for all the inodes
     */
    InodeLockGroup getInodeLockGroup() {
      return mLockGroup;
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
