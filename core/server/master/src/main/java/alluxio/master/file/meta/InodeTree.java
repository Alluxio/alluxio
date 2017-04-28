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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.FieldIndex;
import alluxio.collections.IndexDefinition;
import alluxio.collections.UniqueFieldIndex;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.master.block.ContainerIdGenerable;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.NoopJournalContext;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.Journal;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.SecurityUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents the tree of Inode's.
 */
@NotThreadSafe
// TODO(jiri): Make this class thread-safe.
public class InodeTree implements JournalEntryIterable {
  private static final Logger LOG = LoggerFactory.getLogger(InodeTree.class);
  /** The base amount (exponential backoff) to sleep before retrying persisting an inode. */
  private static final int PERSIST_WAIT_BASE_SLEEP_MS = 2;
  /** Maximum amount (exponential backoff) to sleep before retrying persisting an inode. */
  private static final int PERSIST_WAIT_MAX_SLEEP_MS = 1000;
  /** The maximum retries for persisting an inode. */
  private static final int PERSIST_WAIT_MAX_RETRIES = 50;

  /** Value to be used for an inode with no parent. */
  public static final long NO_PARENT = -1;

  private static final IndexDefinition<Inode<?>> ID_INDEX = new IndexDefinition<Inode<?>>(true) {
    @Override
    public Object getFieldValue(Inode<?> o) {
      return o.getId();
    }
  };

  /**
   * The type of lock to lock inode paths with.
   */
  public enum LockMode {
    /** Read lock the entire path. */
    READ,
    /** Read lock the entire path, but write lock the target inode. */
    WRITE,
    /** Read lock the entire path, but write lock the target inode and the parent of the target. */
    WRITE_PARENT,
  }

  /** Only the root inode should have the empty string as its name. */
  private static final String ROOT_INODE_NAME = "";
  /** Number of retries when trying to lock a path, from a given id. */
  private static final int PATH_TRAVERSAL_RETRIES = 1000;

  /** The root of the entire file system. */
  private InodeDirectory mRoot = null;

  /** Mount table manages the file system mount points. */
  private final MountTable mMountTable;

  /** Use UniqueFieldIndex directly for ID index rather than using IndexedSet. */
  private final FieldIndex<Inode<?>> mInodes = new UniqueFieldIndex<>(ID_INDEX);
  /** A set of inode ids representing pinned inode files. */
  private final Set<Long> mPinnedInodeFileIds = new ConcurrentHashSet<>(64, 0.90f, 64);

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
   * @param owner the root owner
   * @param group the root group
   * @param mode the root mode
   */
  public void initializeRoot(String owner, String group, Mode mode) {
    if (mRoot == null) {
      mRoot = InodeDirectory
          .create(mDirectoryIdGenerator.getNewDirectoryId(), NO_PARENT, ROOT_INODE_NAME,
              CreateDirectoryOptions.defaults().setOwner(owner).setGroup(group).setMode(mode));
      mRoot.setPersistenceState(PersistenceState.PERSISTED);
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
    return mRoot.getOwner();
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
    return mInodes.containsField(id);
  }

  /**
   * @param uri the {@link AlluxioURI} to check for existence
   * @return whether the inode exists
   */
  public boolean inodePathExists(AlluxioURI uri) {
    try {
      TraversalResult traversalResult =
          traverseToInode(PathUtils.getPathComponents(uri.getPath()), LockMode.READ, null);
      traversalResult.getInodeLockList().close();
      return traversalResult.isFound();
    } catch (InvalidPathException e) {
      return false;
    }
  }

  /**
   * Locks existing inodes on the specified path, in the specified {@link LockMode}. The target
   * inode is not required to exist.
   *
   * @param path the path to lock
   * @param lockMode the {@link LockMode} to lock the inodes with
   * @return the {@link LockedInodePath} representing the locked path of inodes
   * @throws InvalidPathException if the path is invalid
   */
  public LockedInodePath lockInodePath(AlluxioURI path, LockMode lockMode)
      throws InvalidPathException {
    TraversalResult traversalResult =
        traverseToInode(PathUtils.getPathComponents(path.getPath()), lockMode, null);
    return new MutableLockedInodePath(path, traversalResult.getInodes(),
        traversalResult.getInodeLockList(), lockMode);
  }

  /**
   * Locks existing inodes on the two specified paths. The two paths will be locked in the
   * correct order. The target inodes are not required to exist.
   *
   * @param path1 the first path to lock
   * @param lockMode1 the {@link LockMode} of the first path
   * @param path2 the second path to lock
   * @param lockMode2 the {@link LockMode} of the second path
   * @return a {@link InodePathPair} representing the two locked paths
   * @throws InvalidPathException if a path is invalid
   */
  public InodePathPair lockInodePathPair(AlluxioURI path1, LockMode lockMode1, AlluxioURI path2,
      LockMode lockMode2) throws InvalidPathException {
    String[] pathComponents1 = PathUtils.getPathComponents(path1.getPath());
    String[] pathComponents2 = PathUtils.getPathComponents(path2.getPath());
    List<LockMode> lockHints = new ArrayList<>();

    int minLength = Math.min(pathComponents1.length, pathComponents2.length);
    for (int i = 0; i < minLength; i++) {
      if (pathComponents1[i].equals(pathComponents2[i])) {
        // The two paths share a common path prefix.
        LockMode mode1 = getLockModeForComponent(i, pathComponents1.length, lockMode1, null);
        LockMode mode2 = getLockModeForComponent(i, pathComponents2.length, lockMode2, null);
        // If either of the modes are WRITE, lock both components as WRITE to prevent deadlock.
        // TODO(gpang): consider a combine helper method
        if (mode1 == LockMode.READ && mode2 == LockMode.READ) {
          lockHints.add(LockMode.READ);
        } else {
          lockHints.add(LockMode.WRITE);
        }
      } else {
        // The two paths no longer share a common prefix.
        break;
      }
    }

    TraversalResult traversalResult1 = null;
    TraversalResult traversalResult2 = null;
    boolean valid = false;
    try {
      // Lock paths in a deterministic order.
      if (path1.compareTo(path2) > 0) {
        traversalResult2 = traverseToInode(pathComponents2, lockMode2, lockHints);
        traversalResult1 = traverseToInode(pathComponents1, lockMode1, lockHints);
      } else {
        traversalResult1 = traverseToInode(pathComponents1, lockMode1, lockHints);
        traversalResult2 = traverseToInode(pathComponents2, lockMode2, lockHints);
      }

      LockedInodePath inodePath1 = new MutableLockedInodePath(path1, traversalResult1.getInodes(),
          traversalResult1.getInodeLockList(), lockMode1);
      LockedInodePath inodePath2 = new MutableLockedInodePath(path2, traversalResult2.getInodes(),
          traversalResult2.getInodeLockList(), lockMode2);
      valid = true;
      return new InodePathPair(inodePath1, inodePath2);
    } finally {
      if (!valid) {
        if (traversalResult1 != null) {
          traversalResult1.getInodeLockList().close();
        }
        if (traversalResult2 != null) {
          traversalResult2.getInodeLockList().close();
        }
      }
    }
  }

  /**
   * Returns the lock mode for a particular index into the path components.
   *
   * @param index the index into the path components
   * @param length the length of path components
   * @param lockMode the specified {@link LockMode}
   * @param lockHints the list of lock hints for each index; this can be null, or incomplete
   * @return the {@link LockMode} to lock this particular inode at this index with
   */
  private LockMode getLockModeForComponent(int index, int length, LockMode lockMode,
      List<LockMode> lockHints) {
    if (lockHints != null && index < lockHints.size()) {
      // Use the lock hint if it exists.
      return lockHints.get(index);
    }
    if (lockMode == LockMode.READ) {
      return LockMode.READ;
    }
    boolean isTarget = (index == length - 1);
    boolean isTargetOrParent = (index >= length - 2);

    if (isTargetOrParent && lockMode == LockMode.WRITE_PARENT
        || isTarget && lockMode == LockMode.WRITE) {
      return LockMode.WRITE;
    }
    return LockMode.READ;
  }

  /**
   * Locks existing inodes on the specified path, in the specified {@link LockMode}. The target
   * inode must exist.
   *
   * @param path the {@link AlluxioURI} path to lock
   * @param lockMode the {@link LockMode} to lock the inodes with
   * @return the {@link LockedInodePath} representing the locked path of inodes
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the target inode does not exist
   */
  public LockedInodePath lockFullInodePath(AlluxioURI path, LockMode lockMode)
      throws InvalidPathException, FileDoesNotExistException {
    TraversalResult traversalResult =
        traverseToInode(PathUtils.getPathComponents(path.getPath()), lockMode, null);
    if (!traversalResult.isFound()) {
      traversalResult.getInodeLockList().close();
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
    return new MutableLockedInodePath(path, traversalResult.getInodes(),
        traversalResult.getInodeLockList(), lockMode);
  }

  /**
   * Locks existing inodes on the path to the inode specified by an id, in the specified
   * {@link LockMode}. The target inode must exist. This may require multiple traversals of the
   * tree, so may be inefficient.
   *
   * @param id the inode id
   * @param lockMode the {@link LockMode} to lock the inodes with
   * @return the {@link LockedInodePath} representing the locked path of inodes
   * @throws FileDoesNotExistException if the target inode does not exist
   */
  public LockedInodePath lockFullInodePath(long id, LockMode lockMode)
      throws FileDoesNotExistException {
    int count = 0;
    while (true) {
      Inode<?> inode = mInodes.getFirst(id);
      if (inode == null) {
        throw new FileDoesNotExistException(ExceptionMessage.INODE_DOES_NOT_EXIST.getMessage(id));
      }
      // Compute the path given the target inode.
      StringBuilder builder = new StringBuilder();
      computePathForInode(inode, builder);
      AlluxioURI uri = new AlluxioURI(builder.toString());

      boolean valid = false;
      LockedInodePath inodePath = null;
      try {
        inodePath = lockFullInodePath(uri, lockMode);
        if (inodePath.getInode().getId() == id) {
          // Set to true, so the path is not unlocked before returning.
          valid = true;
          return inodePath;
        }
        // The path does not end up at the target inode id. Repeat the traversal.
      } catch (InvalidPathException e) {
        // ignore and repeat the loop
        LOG.warn("Inode lookup id {} computed path {} mismatch id. Repeating.", id, uri);
      } finally {
        if (!valid && inodePath != null) {
          inodePath.close();
        }
      }
      count++;
      if (count > PATH_TRAVERSAL_RETRIES) {
        throw new FileDoesNotExistException(
            ExceptionMessage.INODE_DOES_NOT_EXIST_RETRIES.getMessage(id));
      }
    }
  }

  /**
   * Attempts to extend an existing {@link LockedInodePath} to reach the target inode (the last
   * inode for the full path). If the target inode does not exist, an exception will be thrown.
   *
   * @param inodePath the {@link LockedInodePath} to extend to the target inode
   * @param lockMode the {@link LockMode} to lock the inodes with
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the target inode does not exist
   */
  public void ensureFullInodePath(LockedInodePath inodePath, LockMode lockMode)
      throws InvalidPathException, FileDoesNotExistException {
    if (inodePath.fullPathExists()) {
      return;
    }
    TraversalResult traversalResult = traverseToInode(inodePath, lockMode);
    if (!traversalResult.isFound()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(inodePath.getUri()));
    }
  }

  /**
   * Appends components of the path from a given inode.
   *
   * @param inode the {@link Inode} to compute the path for
   * @param builder a {@link StringBuilder} that is updated with the path components
   * @throws FileDoesNotExistException if an inode in the path does not exist
   */
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
      Inode<?> parentInode = mInodes.getFirst(parentId);
      if (parentInode == null) {
        throw new FileDoesNotExistException(
            ExceptionMessage.INODE_DOES_NOT_EXIST.getMessage(parentId));
      }

      computePathForInode(parentInode, builder);
      builder.append(AlluxioURI.SEPARATOR);
      builder.append(name);
    }
  }

  /**
   * Returns the path for a particular inode. The inode and the path to the inode must already be
   * locked.
   *
   * @param inode the {@link Inode} to get the path for
   * @return the {@link AlluxioURI} for the path of the inode
   * @throws FileDoesNotExistException if the path does not exist
   */
  public AlluxioURI getPath(Inode<?> inode) throws FileDoesNotExistException {
    Preconditions.checkState(inode.isWriteLocked() || inode.isReadLocked());
    StringBuilder builder = new StringBuilder();
    computePathForInode(inode, builder);
    return new AlluxioURI(builder.toString());
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
   * @param inodePath the path
   * @param options method options
   * @param journalContext the journal context
   * @return a {@link CreatePathResult} representing the modified inodes and created inodes during
   *         path creation
   * @throws FileAlreadyExistsException when there is already a file at path if we want to create a
   *         directory there
   * @throws BlockInfoException when blockSizeBytes is invalid
   * @throws InvalidPathException when path is invalid, for example, (1) when there is nonexistent
   *         necessary parent directories and recursive is false, (2) when one of the necessary
   *         parent directories is actually a file
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  public CreatePathResult createPath(LockedInodePath inodePath, CreatePathOptions<?> options,
      JournalContext journalContext)
      throws FileAlreadyExistsException, BlockInfoException, InvalidPathException, IOException,
      FileDoesNotExistException {
    // TODO(gpang): consider splitting this into createFilePath and createDirectoryPath, with a
    // helper method for the shared logic.
    AlluxioURI path = inodePath.getUri();
    if (path.isRoot()) {
      String errorMessage = ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path);
      LOG.error(errorMessage);
      throw new FileAlreadyExistsException(errorMessage);
    }
    if (options instanceof CreateFileOptions) {
      CreateFileOptions fileOptions = (CreateFileOptions) options;
      if (fileOptions.getBlockSizeBytes() < 1) {
        throw new BlockInfoException("Invalid block size " + fileOptions.getBlockSizeBytes());
      }
    }

    if (!(inodePath instanceof MutableLockedInodePath)) {
      throw new InvalidPathException(
          ExceptionMessage.NOT_MUTABLE_INODE_PATH.getMessage(inodePath.getUri()));
    }

    LOG.debug("createPath {}", path);

    TraversalResult traversalResult = traverseToInode(inodePath, inodePath.getLockMode());
    InodeLockList lockList = traversalResult.getInodeLockList();

    MutableLockedInodePath extensibleInodePath = (MutableLockedInodePath) inodePath;
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
        LOG.error("FileDoesNotExistException: {}", msg);
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

    List<Inode<?>> createdInodes = new ArrayList<>();
    List<Inode<?>> modifiedInodes = new ArrayList<>();
    if (options.isPersisted()) {
      // Synchronously persist directories. These inodes are already READ locked.
      for (Inode inode : traversalResult.getNonPersisted()) {
        syncPersistDirectory((InodeDirectory) inode, journalContext);
      }
    }
    if (pathIndex < (pathComponents.length - 1) || currentInodeDirectory.getChild(name) == null) {
      // (1) There are components in parent paths that need to be created. Or
      // (2) The last component of the path needs to be created.
      // In these two cases, the last traversed Inode will be modified.
      currentInodeDirectory.setLastModificationTimeMs(options.getOperationTimeMs());
      modifiedInodes.add(currentInodeDirectory);

      File.InodeLastModificationTimeEntry inodeLastModificationTime =
          File.InodeLastModificationTimeEntry.newBuilder().setId(currentInodeDirectory.getId())
              .setLastModificationTimeMs(options.getOperationTimeMs()).build();
      journalContext.append(
          Journal.JournalEntry.newBuilder().setInodeLastModificationTime(inodeLastModificationTime)
              .build());
    }

    // Fill in the ancestor directories that were missing.
    // NOTE, we set the mode of missing ancestor directories to be the default value, rather
    // than inheriting the option of the final file to create, because it may not have
    // "execute" permission.
    CreateDirectoryOptions missingDirOptions = CreateDirectoryOptions.defaults()
        .setMountPoint(false)
        .setPersisted(options.isPersisted())
        .setOperationTimeMs(options.getOperationTimeMs())
        .setOwner(options.getOwner())
        .setGroup(options.getGroup());
    for (int k = pathIndex; k < (pathComponents.length - 1); k++) {
      InodeDirectory dir = null;
      while (dir == null) {
        dir = InodeDirectory.create(mDirectoryIdGenerator.getNewDirectoryId(journalContext),
            currentInodeDirectory.getId(), pathComponents[k], missingDirOptions);
        // Lock the newly created inode before subsequent operations, and add it to the lock group.
        lockList.lockWriteAndCheckNameAndParent(dir, currentInodeDirectory, pathComponents[k]);

        if (!currentInodeDirectory.addChild(dir)) {
          // The child directory inode already exists. Get the existing child inode.
          lockList.unlockLast();

          dir =
              (InodeDirectory) currentInodeDirectory.getChildReadLock(pathComponents[k], lockList);
          if (dir == null) {
            // Could not get the child inode. Continue and try again.
            continue;
          }
        } else {
          // Successfully added the child, while holding the write lock.
          dir.setPinned(currentInodeDirectory.isPinned());
          if (options.isPersisted()) {
            // Do not journal the persist entry, since a creation entry will be journaled instead.
            syncPersistDirectory(dir, NoopJournalContext.INSTANCE);
          }
          // Journal the new inode.
          journalContext.append(dir.toJournalEntry());
          mInodes.add(dir);

          // After creation and journaling, downgrade to a read lock.
          lockList.downgradeLast();
        }
      }

      createdInodes.add(dir);
      extensibleInodePath.getInodes().add(dir);
      currentInodeDirectory = dir;
    }

    // Create the final path component. First we need to make sure that there isn't already a file
    // here with that name. If there is an existing file that is a directory and we're creating a
    // directory, update persistence property of the directories if needed, otherwise, throw
    // FileAlreadyExistsException unless options.allowExists is true.
    Inode<?> lastInode = null;
    while (lastInode == null) {
      // Try to lock the last inode with the lock mode of the path.
      switch (extensibleInodePath.getLockMode()) {
        case READ:
          lastInode = currentInodeDirectory.getChildReadLock(name, lockList);
          break;
        case WRITE_PARENT:
        case WRITE:
          lastInode = currentInodeDirectory.getChildWriteLock(name, lockList);
          break;
        default:
          // This should not be reachable.
          LOG.warn("Unexpected lock mode encountered: {}", extensibleInodePath.getLockMode());
      }
      if (lastInode != null) {
        // inode to create already exists
        if (lastInode.isDirectory() && options instanceof CreateDirectoryOptions && !lastInode
            .isPersisted() && options.isPersisted()) {
          // The final path component already exists and is not persisted, so it should be added
          // to the non-persisted Inodes of traversalResult.
          syncPersistDirectory((InodeDirectory) lastInode, journalContext);
        } else if (!lastInode.isDirectory() || !(options instanceof CreateDirectoryOptions
            && ((CreateDirectoryOptions) options).isAllowExists())) {
          String errorMessage = ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path);
          LOG.error(errorMessage);
          throw new FileAlreadyExistsException(errorMessage);
        }
      } else {
        // create the new inode, with a write lock
        if (options instanceof CreateDirectoryOptions) {
          CreateDirectoryOptions directoryOptions = (CreateDirectoryOptions) options;
          lastInode = InodeDirectory.create(mDirectoryIdGenerator.getNewDirectoryId(journalContext),
              currentInodeDirectory.getId(), name, directoryOptions);
          // Lock the created inode before subsequent operations, and add it to the lock group.
          lockList.lockWriteAndCheckNameAndParent(lastInode, currentInodeDirectory, name);
          if (directoryOptions.isPersisted()) {
            // Do not journal the persist entry, since a creation entry will be journaled instead.
            syncPersistDirectory((InodeDirectory) lastInode, NoopJournalContext.INSTANCE);
          }
        } else if (options instanceof CreateFileOptions) {
          CreateFileOptions fileOptions = (CreateFileOptions) options;
          lastInode = InodeFile.create(mContainerIdGenerator.getNewContainerId(),
              currentInodeDirectory.getId(), name, System.currentTimeMillis(), fileOptions);
          // Lock the created inode before subsequent operations, and add it to the lock group.
          lockList.lockWriteAndCheckNameAndParent(lastInode, currentInodeDirectory, name);
          if (fileOptions.isCacheable()) {
            ((InodeFile) lastInode).setCacheable(true);
          }
        }
        lastInode.setPinned(currentInodeDirectory.isPinned());

        if (!currentInodeDirectory.addChild(lastInode)) {
          // Could not add the child inode to the parent. Continue and try again.
          // Cleanup is not necessary, since other state is updated later, after a successful add.
          lockList.unlockLast();
          lastInode = null;
          continue;
        }

        if (lastInode instanceof InodeFile) {
          if (currentInodeDirectory.isPinned()) {
            // Update set of pinned file ids.
            mPinnedInodeFileIds.add(lastInode.getId());
          }
        }

        // Journal the new inode.
        journalContext.append(lastInode.toJournalEntry());

        // Update state while holding the write lock.
        mInodes.add(lastInode);

        if (extensibleInodePath.getLockMode() == LockMode.READ) {
          // After creating the inode, downgrade to a read lock
          lockList.downgradeLast();
        }

        createdInodes.add(lastInode);
        extensibleInodePath.getInodes().add(lastInode);
      }
    }

    LOG.debug("createFile: File Created: {} parent: {}", lastInode, currentInodeDirectory);
    return new CreatePathResult(modifiedInodes, createdInodes);
  }

  /**
   * Reinitializes the block size and TTL of an existing open file.
   *
   * @param inodePath the path to the file
   * @param blockSizeBytes the new block size
   * @param ttl the ttl
   * @param ttlAction action to perform after TTL expiry
   * @return the file id
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the path does not exist
   */
  public long reinitializeFile(LockedInodePath inodePath, long blockSizeBytes, long ttl,
      TtlAction ttlAction)
      throws InvalidPathException, FileDoesNotExistException {
    InodeFile file = inodePath.getInodeFile();
    file.setBlockSizeBytes(blockSizeBytes);
    file.setTtl(ttl);
    file.setTtlAction(ttlAction);
    return file.getId();
  }

  /**
   * Locks all descendants of a particular {@link LockedInodePath}. Any directory inode
   * precedes its descendants in the list.
   *
   * @param inodePath the root {@link LockedInodePath} to retrieve all descendants from
   * @param lockMode the lock type to use
   * @return an {@link InodeLockList} representing the list of all descendants
   * @throws FileDoesNotExistException if inode does not exist
   */
  public InodeLockList lockDescendants(LockedInodePath inodePath, LockMode lockMode)
      throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
    InodeLockList inodeGroup = new InodeLockList();
    if (!inode.isDirectory()) {
      return inodeGroup;
    }
    return lockDescendantsInternal((InodeDirectory) inode, lockMode, inodeGroup);
  }

  private InodeLockList lockDescendantsInternal(InodeDirectory inodeDirectory,
      LockMode lockMode, InodeLockList inodeGroup) {
    for (Inode<?> child : inodeDirectory.getChildren()) {
      if (lockMode == LockMode.READ) {
        try {
          inodeGroup.lockReadAndCheckParent(child, inodeDirectory);
        } catch (InvalidPathException e) {
          // Inode is no longer a child, continue.
          continue;
        }
      } else {
        try {
          inodeGroup.lockWriteAndCheckParent(child, inodeDirectory);
        } catch (InvalidPathException e) {
          // Inode is no longer a child, continue.
          continue;
        }
      }
      if (child.isDirectory()) {
        lockDescendantsInternal((InodeDirectory) child, lockMode, inodeGroup);
      }
    }
    return inodeGroup;
  }

  /**
   * Deletes a single inode from the inode tree by removing it from the parent inode.
   *
   * @param inodePath The {@link LockedInodePath} to delete
   * @param opTimeMs The operation time
   * @param deleteOptions the delete options
   * @param journalContext the journal context
   * @throws FileDoesNotExistException if the Inode cannot be retrieved
   */
  public void deleteInode(LockedInodePath inodePath, long opTimeMs, DeleteOptions deleteOptions,
      JournalContext journalContext)
      throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
    InodeDirectory parent = (InodeDirectory) mInodes.getFirst(inode.getParentId());
    if (parent == null) {
      LOG.warn("Parent id not found: {} deleting inode: {}", inode.getParentId(), inode);
      throw new FileDoesNotExistException(
          ExceptionMessage.INODE_DOES_NOT_EXIST.getMessage(inode.getParentId()));
    }

    // Journal before removing the inode from the parent, since the parent is read locked.
    File.DeleteFileEntry deleteFile = File.DeleteFileEntry.newBuilder().setId(inode.getId())
        .setAlluxioOnly(deleteOptions.isAlluxioOnly())
        .setRecursive(deleteOptions.isRecursive())
        .setOpTimeMs(opTimeMs).build();
    journalContext.append(Journal.JournalEntry.newBuilder().setDeleteFile(deleteFile).build());

    parent.removeChild(inode);
    parent.setLastModificationTimeMs(opTimeMs);

    mInodes.remove(inode);
    mPinnedInodeFileIds.remove(inode.getId());
    inode.setDeleted(true);
  }

  /**
   * Sets the pinned state of an inode. If the inode is a directory, the pinned state will be set
   * recursively.
   *
   * @param inodePath the {@link LockedInodePath} to set the pinned state for
   * @param pinned the pinned state to set for the inode (and possible descendants)
   * @param opTimeMs the operation time
   * @throws FileDoesNotExistException if inode does not exist
   */
  public void setPinned(LockedInodePath inodePath, boolean pinned, long opTimeMs)
      throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
    inode.setPinned(pinned);
    inode.setLastModificationTimeMs(opTimeMs);

    if (inode.isFile()) {
      InodeFile inodeFile = (InodeFile) inode;
      if (inodeFile.isPinned()) {
        mPinnedInodeFileIds.add(inodeFile.getId());
      } else {
        mPinnedInodeFileIds.remove(inodeFile.getId());
      }
    } else {
      assert inode instanceof InodeDirectory;
      // inode is a directory. Set the pinned state for all children.
      TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(inodePath);
      for (Inode<?> child : ((InodeDirectory) inode).getChildren()) {
        try {
          child.lockWriteAndCheckParent(inode);
        } catch (InvalidPathException e) {
          // Inode is no longer a child of the directory, continue.
          continue;
        }
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
   * @param inodePath the {@link LockedInodePath} to set the pinned state for
   * @param pinned the pinned state to set for the inode (and possible descendants)
   * @throws FileDoesNotExistException if inode does not exist
   */
  public void setPinned(LockedInodePath inodePath, boolean pinned)
      throws FileDoesNotExistException {
    setPinned(inodePath, pinned, System.currentTimeMillis());
  }

  /**
   * @return the set of file ids which are pinned
   */
  public Set<Long> getPinIdSet() {
    return new HashSet<>(mPinnedInodeFileIds);
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
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    // Write tree via breadth-first traversal, so that during deserialization, it may be more
    // efficient than depth-first during deserialization due to parent directory's locality.
    final Queue<Inode<?>> inodes = new LinkedList<>();
    inodes.add(mRoot);
    return new Iterator<Journal.JournalEntry>() {
      @Override
      public boolean hasNext() {
        return !inodes.isEmpty();
      }

      @Override
      public Journal.JournalEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Inode<?> inode = inodes.poll();
        if (inode.isDirectory()) {
          inodes.addAll(((InodeDirectory) inode).getChildren());
        }
        return inode.toJournalEntry();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("InodeTree#Iterator#remove is not supported");
      }
    };
  }

  /**
   * Adds the file represented by the entry parameter into the inode tree.
   *
   * @param entry the journal entry representing an inode
   */
  public void addInodeFileFromJournal(InodeFileEntry entry) {
    InodeFile file = InodeFile.fromJournalEntry(entry);
    addInodeFromJournalInternal(file);
  }

  /**
   * Adds the directory represented by the entry parameter into the inode tree. If the inode entry
   * represents the root inode, the tree is "reset", and all state is cleared.
   *
   * @param entry the journal entry representing an inode
   * @throws AccessControlException when owner of mRoot is not the owner of root journal entry
   */
  public void addInodeDirectoryFromJournal(InodeDirectoryEntry entry)
      throws AccessControlException {
    InodeDirectory directory = InodeDirectory.fromJournalEntry(entry);
    if (directory.getName().equals(ROOT_INODE_NAME)) {
      // This is the root inode. Clear all the state, and set the root.
      // For backwards-compatibility:
      // Empty owner in journal entry indicates that previous journal has no security. In this
      // case, the journal is allowed to be applied to the new inode with security turned on.
      if (SecurityUtils.isSecurityEnabled() && mRoot != null && !directory.getOwner().isEmpty()
          && !mRoot.getOwner().equals(directory.getOwner())) {
        // user is not the owner of journal root entry
        throw new AccessControlException(
            ExceptionMessage.PERMISSION_DENIED.getMessage("Unauthorized user on root"));
      }
      mInodes.clear();
      mPinnedInodeFileIds.clear();
      mRoot = directory;
      // If journal entry has no security enabled, change the replayed inode permission to be 0777
      // for backwards-compatibility.
      if (SecurityUtils.isSecurityEnabled() && mRoot != null && mRoot.getOwner().isEmpty() && mRoot
          .getGroup().isEmpty()) {
        mRoot.setMode(Constants.DEFAULT_FILE_SYSTEM_MODE);
      }
      mCachedInode = mRoot;
      mInodes.add(mRoot);
    } else {
      addInodeFromJournalInternal(directory);
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
      parentDirectory = (InodeDirectory) mInodes.getFirst(inode.getParentId());
      mCachedInode = parentDirectory;
    }
    parentDirectory.addChild(inode);
    mInodes.add(inode);
    // If journal entry has no security enabled, change the replayed inode permission to be 0777
    // for backwards-compatibility.
    if (SecurityUtils.isSecurityEnabled() && inode != null && inode.getOwner().isEmpty()
        && inode.getGroup().isEmpty()) {
      inode.setMode(Constants.DEFAULT_FILE_SYSTEM_MODE);
    }
    // Update indexes.
    if (inode.isFile() && inode.isPinned()) {
      mPinnedInodeFileIds.add(inode.getId());
    }
  }

  /**
   * Synchronously persists an {@link InodeDirectory} to the UFS. If concurrent calls are made, only
   * one thread will persist to UFS, and the others will wait until it is persisted.
   *
   * @param dir the {@link InodeDirectory} to persist
   * @param journalContext the journal context
   * @throws InvalidPathException if the path for the inode is invalid
   * @throws FileDoesNotExistException if the path for the inode is invalid
   */
  public void syncPersistDirectory(InodeDirectory dir, JournalContext journalContext)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    RetryPolicy retry =
        new ExponentialBackoffRetry(PERSIST_WAIT_BASE_SLEEP_MS, PERSIST_WAIT_MAX_SLEEP_MS,
            PERSIST_WAIT_MAX_RETRIES);
    while (dir.getPersistenceState() != PersistenceState.PERSISTED) {
      if (dir.compareAndSwap(PersistenceState.NOT_PERSISTED,
          PersistenceState.TO_BE_PERSISTED)) {
        boolean success = false;
        try {
          AlluxioURI uri = getPath(dir);
          MountTable.Resolution resolution = mMountTable.resolve(uri);
          String ufsUri = resolution.getUri().toString();
          UnderFileSystem ufs = resolution.getUfs();
          MkdirsOptions mkdirsOptions =
              MkdirsOptions.defaults().setCreateParent(false).setOwner(dir.getOwner())
                  .setGroup(dir.getGroup()).setMode(new Mode(dir.getMode()));
          ufs.mkdirs(ufsUri, mkdirsOptions);
          dir.setPersistenceState(PersistenceState.PERSISTED);

          // Append the persist entry to the journal.
          File.PersistDirectoryEntry persistDirectory =
              File.PersistDirectoryEntry.newBuilder().setId(dir.getId()).build();
          journalContext.append(
              Journal.JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
          success = true;
        } finally {
          if (!success) {
            // Failed to persist the inode, so set the state back to NOT_PERSISTED.
            dir.setPersistenceState(PersistenceState.NOT_PERSISTED);
          }
        }
      } else {
        if (!retry.attemptRetry()) {
          throw new IOException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(dir.getName()));
        }
      }
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRoot, mInodes, mPinnedInodeFileIds, mContainerIdGenerator,
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
        && Objects.equal(mInodes, that.mInodes)
        && Objects.equal(mPinnedInodeFileIds, that.mPinnedInodeFileIds)
        && Objects.equal(mContainerIdGenerator, that.mContainerIdGenerator)
        && Objects.equal(mDirectoryIdGenerator, that.mDirectoryIdGenerator)
        && Objects.equal(mCachedInode, that.mCachedInode);
  }

  /**
   * Traverses the tree to find the given path components. Hints for the lock mode at each path
   * component can be specified.
   *
   * @param pathComponents the components of the path to traverse
   * @param lockMode the {@link LockMode} for the path
   * @param lockHints optional {@link List} to specify the lock type for each path component; this
   *                  can be shorter than pathComponents
   * @return the {@link TraversalResult} for this traversal
   * @throws InvalidPathException if the path is invalid
   */
  private TraversalResult traverseToInode(String[] pathComponents, LockMode lockMode,
      List<LockMode> lockHints)
      throws InvalidPathException {
    List<Inode<?>> nonPersistedInodes = new ArrayList<>();
    List<Inode<?>> inodes = new ArrayList<>();
    InodeLockList lockList = new InodeLockList();

    // This must be set to true before returning a valid value, otherwise all the inodes will be
    // unlocked.
    boolean valid = false;
    try {
      if (pathComponents == null) {
        throw new InvalidPathException(ExceptionMessage.PATH_COMPONENTS_INVALID.getMessage("null"));
      } else if (pathComponents.length == 0) {
        throw new InvalidPathException(
            ExceptionMessage.PATH_COMPONENTS_INVALID.getMessage("empty"));
      } else if (pathComponents.length == 1) {
        if (pathComponents[0].equals("")) {
          if (getLockModeForComponent(0, pathComponents.length, lockMode, lockHints)
              == LockMode.READ) {
            lockList.lockRead(mRoot);
          } else {
            lockList.lockWrite(mRoot);
          }
          inodes.add(mRoot);
          valid = true;
          return TraversalResult.createFoundResult(nonPersistedInodes, inodes, lockList);
        } else {
          throw new InvalidPathException(
              ExceptionMessage.PATH_COMPONENTS_INVALID_START.getMessage(pathComponents[0]));
        }
      }

      if (getLockModeForComponent(0, pathComponents.length, lockMode, lockHints) == LockMode.READ) {
        lockList.lockRead(mRoot);
      } else {
        lockList.lockWrite(mRoot);
      }
      inodes.add(mRoot);
      TraversalResult result =
          traverseToInodeInternal(pathComponents, inodes, nonPersistedInodes, lockList, lockMode,
              lockHints);
      valid = true;
      return result;
    } finally {
      if (!valid) {
        lockList.close();
      }
    }
  }

  /**
   * Traverses the tree to find the rest of the given {@link LockedInodePath}. Hints for the lock
   * mode at each path.
   *
   * @param inodePath the {@link LockedInodePath} to start the traversal from
   * @param lockMode the {@link LockMode} for the path
   * @return the {@link TraversalResult} for this traversal
   * @throws InvalidPathException if the path is invalid
   */
  private TraversalResult traverseToInode(LockedInodePath inodePath, LockMode lockMode)
      throws InvalidPathException {
    // the inodePath is guaranteed to already include at least the root inode.
    if (!(inodePath instanceof MutableLockedInodePath)) {
      throw new InvalidPathException(
          ExceptionMessage.NOT_MUTABLE_INODE_PATH.getMessage(inodePath.getUri()));
    }
    MutableLockedInodePath extensibleInodePath = (MutableLockedInodePath) inodePath;
    List<Inode<?>> inodes = extensibleInodePath.getInodes();
    InodeLockList lockList = extensibleInodePath.getLockList();
    List<Inode<?>> nonPersistedInodes = new ArrayList<>();
    for (Inode<?> inode : inodes) {
      if (!inode.isPersisted()) {
        nonPersistedInodes.add(inode);
      }
    }
    return traverseToInodeInternal(extensibleInodePath.getPathComponents(), inodes,
        nonPersistedInodes, lockList, lockMode, null);
  }

  private TraversalResult traverseToInodeInternal(String[] pathComponents, List<Inode<?>> inodes,
      List<Inode<?>> nonPersistedInodes, InodeLockList lockList, LockMode lockMode,
      List<LockMode> lockHints)
      throws InvalidPathException {
    Inode<?> current = inodes.get(inodes.size() - 1);
    for (int i = inodes.size(); i < pathComponents.length; i++) {
      Inode<?> next = ((InodeDirectory) current).getChild(pathComponents[i]);
      if (next == null) {
        // The user might want to create the nonexistent directories, so return the traversal
        // result current inode with the last Inode taken, and the index of the first path
        // component that couldn't be found.
        return TraversalResult.createNotFoundResult(i, nonPersistedInodes, inodes, lockList);
      }
      // Lock the existing next inode before proceeding.
      if (getLockModeForComponent(i, pathComponents.length, lockMode, lockHints)
          == LockMode.READ) {
        lockList.lockReadAndCheckNameAndParent(next, current, pathComponents[i]);
      } else {
        lockList.lockWriteAndCheckNameAndParent(next, current, pathComponents[i]);
      }
      if (next.isFile()) {
        // The inode can't have any children. If this is the last path component, we're good.
        // Otherwise, we can't traverse further, so we clean up and throw an exception.
        if (i == pathComponents.length - 1) {
          inodes.add(next);
          return TraversalResult.createFoundResult(nonPersistedInodes, inodes, lockList);
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
    return TraversalResult.createFoundResult(nonPersistedInodes, inodes, lockList);
  }

  private static final class TraversalResult {
    /** True if the traversal found the target inode, false otherwise. */
    private final boolean mFound;

    /** The list of non-persisted inodes encountered during the traversal. */
    private final List<Inode<?>> mNonPersisted;

    /** The list of all inodes encountered during the traversal. */
    private final List<Inode<?>> mInodes;

    /** The {@link InodeLockList} managing the locks for the inodes. */
    private final InodeLockList mLockList;

    // TODO(gpang): consider a builder paradigm to iteratively build the traversal result.
    static TraversalResult createFoundResult(List<Inode<?>> nonPersisted, List<Inode<?>> inodes,
        InodeLockList lockList) {
      return new TraversalResult(true, nonPersisted, inodes, lockList);
    }

    static TraversalResult createNotFoundResult(int index, List<Inode<?>> nonPersisted,
        List<Inode<?>> inodes, InodeLockList lockList) {
      return new TraversalResult(false, nonPersisted, inodes, lockList);
    }

    private TraversalResult(boolean found, List<Inode<?>> nonPersisted,
        List<Inode<?>> inodes, InodeLockList lockList) {
      mFound = found;
      mNonPersisted = nonPersisted;
      mInodes = inodes;
      mLockList = lockList;
    }

    /**
     * @return true if target inode was found, false otherwise
     */
    boolean isFound() {
      return mFound;
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
     * @return the {@link InodeLockList} managing the locks for all the inodes
     */
    InodeLockList getInodeLockList() {
      return mLockList;
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

    /**
     * Constructs the results of modified and created inodes when creating a path.
     *
     * @param modified a list of modified inodes
     * @param created a list of created inodes
     */
    CreatePathResult(List<Inode<?>> modified, List<Inode<?>> created) {
      mModified = Preconditions.checkNotNull(modified);
      mCreated = Preconditions.checkNotNull(created);
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
  }
}
