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
import alluxio.collections.Pair;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.master.block.ContainerIdGenerable;
import alluxio.master.file.RpcContext;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.file.state.InodesView;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.JournalEntryReplayable;
import alluxio.proto.journal.File.DeleteFileEntry;
import alluxio.proto.journal.File.NewBlockEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAclEntry;
import alluxio.proto.journal.File.UpdateInodeDirectoryEntry;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableResource;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.interfaces.Scoped;
import alluxio.util.io.PathUtils;

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
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents the tree of Inodes.
 */
@NotThreadSafe
// TODO(jiri): Make this class thread-safe.
public class InodeTree implements JournalEntryIterable, JournalEntryReplayable {
  private static final Logger LOG = LoggerFactory.getLogger(InodeTree.class);
  /** The base amount (exponential backoff) to sleep before retrying persisting an inode. */
  private static final int PERSIST_WAIT_BASE_SLEEP_MS = 2;
  /** Maximum amount (exponential backoff) to sleep before retrying persisting an inode. */
  private static final int PERSIST_WAIT_MAX_SLEEP_MS = 1000;
  /** The maximum retries for persisting an inode. */
  private static final int PERSIST_WAIT_MAX_RETRIES = 50;

  /** Value to be used for an inode with no parent. */
  public static final long NO_PARENT = -1;

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
  public static final String ROOT_INODE_NAME = "";

  /** Number of retries when trying to lock a path, from a given id. */
  private static final int PATH_TRAVERSAL_RETRIES = 1000;

  /** Mount table manages the file system mount points. */
  private final MountTable mMountTable;

  private final TtlBucketList mTtlBuckets;
  /** Unmodifiable view of all inodes in the inode tree. */
  private final InodesView mInodes;
  /**
   * Class for managing the persistent state of the inode tree. All metadata changes must go
   * through this class by calling mState.applyAndJournal(context, entry).
   */
  private final InodeTreePersistentState mState;

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
   * @param containerIdGenerator the container id generator to use to get new container ids
   * @param directoryIdGenerator the directory id generator to use to get new directory ids
   * @param mountTable the mount table to manage the file system mount points
   */
  public InodeTree(ContainerIdGenerable containerIdGenerator,
      InodeDirectoryIdGenerator directoryIdGenerator, MountTable mountTable) {
    mTtlBuckets = new TtlBucketList();
    mState = new InodeTreePersistentState(mTtlBuckets);
    mInodes = mState.getInodesView();
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
   * @param context the journal context to journal the initialization to
   */
  public void initializeRoot(String owner, String group, Mode mode, JournalContext context)
      throws UnavailableException {
    if (mState.getRoot() == null) {
      InodeDirectory root = InodeDirectory.create(mDirectoryIdGenerator.getNewDirectoryId(context),
          NO_PARENT, ROOT_INODE_NAME,
          CreateDirectoryOptions.defaults().setOwner(owner).setGroup(group).setMode(mode));
      root.setPersistenceState(PersistenceState.PERSISTED);
      mState.applyAndJournal(context, root);
    }
  }

  /**
   * Applies a journal entry to the inode tree state. This method should only be used during journal
   * replay.
   *
   * @param entry an entry to apply to the inode tree
   * @return whether the journal entry was of a type recognized by the inode tree
   */
  public boolean replayJournalEntryFromJournal(JournalEntry entry) {
    return mState.replayJournalEntryFromJournal(entry);
  }

  /**
   * @return the list of TTL buckets for tracking inode TTLs
   */
  public TtlBucketList getTtlBuckets() {
    return mTtlBuckets;
  }

  /**
   * Marks an inode directory as having its direct children loaded.
   *
   * @param context journal context supplier
   * @param dir the inode directory
   */
  public void setDirectChildrenLoaded(Supplier<JournalContext> context, InodeDirectoryView dir) {
    mState.applyAndJournal(context, UpdateInodeDirectoryEntry.newBuilder()
        .setId(dir.getId())
        .setDirectChildrenLoaded(true)
        .build());
  }

  /**
   * @param context journal context supplier
   * @param entry an entry representing an add block operation
   * @return the new block id
   */
  public long newBlock(Supplier<JournalContext> context, NewBlockEntry entry) {
    return mState.applyAndJournal(context, entry);
  }

  /**
   * @param context journal context supplier
   * @param entry an entry representing an update inode file operation
   */
  public void updateInodeFile(Supplier<JournalContext> context, UpdateInodeFileEntry entry) {
    mState.applyAndJournal(context, entry);
  }

  /**
   * @param context journal context supplier
   * @param entry an entry representing an update inode operation
   */
  public void updateInode(Supplier<JournalContext> context, UpdateInodeEntry entry) {
    mState.applyAndJournal(context, entry);
  }

  /**
   * @param context journal context supplier
   * @param entry an entry representing a rename operation
   * @return whether the operation succeeded
   */
  public boolean rename(Supplier<JournalContext> context, RenameEntry entry) {
    return mState.applyAndJournal(context, entry);
  }

  /**
   * @param context journal context supplier
   * @param entry an entry representing a set acl operation
   */
  public void setAcl(Supplier<JournalContext> context, SetAclEntry entry) {
    mState.applyAndJournal(context, entry);
  }

  /**
   * @return username of root of inode tree, null if the inode tree is not initialized
   */
  @Nullable
  public String getRootUserName() {
    if (mState.getRoot() == null) {
      return null;
    }
    return mState.getRoot().getOwner();
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
    return mState.getPinnedInodeFileIds().size();
  }

  /**
   * @param id the id to get the inode for
   * @return whether the inode exists
   */
  public boolean inodeIdExists(long id) {
    return mInodes.containsId(id);
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
    return new MutableLockedInodePath(path,
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

      LockedInodePath inodePath1 = new MutableLockedInodePath(path1,
          traversalResult1.getInodeLockList(), lockMode1);
      LockedInodePath inodePath2 = new MutableLockedInodePath(path2,
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
    return new MutableLockedInodePath(path,
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
      InodeView inode = mInodes.getById(id);
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
  private void computePathForInode(InodeView inode, StringBuilder builder)
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
      InodeView parentInode = mInodes.getById(parentId);
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
  public AlluxioURI getPath(InodeView inode) throws FileDoesNotExistException {
    Preconditions.checkState(inode.isWriteLocked() || inode.isReadLocked());
    StringBuilder builder = new StringBuilder();
    computePathForInode(inode, builder);
    return new AlluxioURI(builder.toString());
  }

  /**
   * @return the root inode
   */
  public InodeDirectoryView getRoot() {
    return mState.getRoot();
  }

  /**
   * Creates a file or directory at path.
   *
   * @param rpcContext the rpc context
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
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  public CreatePathResult createPath(RpcContext rpcContext, LockedInodePath inodePath,
      CreatePathOptions<?> options) throws FileAlreadyExistsException, BlockInfoException,
      InvalidPathException, IOException, FileDoesNotExistException {
    // TODO(gpang): consider splitting this into createFilePath and createDirectoryPath, with a
    // helper method for the shared logic.
    AlluxioURI path = inodePath.getUri();
    if (path.isRoot()) {
      String errorMessage = ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path);
      LOG.error(errorMessage);
      throw new FileAlreadyExistsException(errorMessage);
    }
    if (inodePath.fullPathExists()) {
      if (!(options instanceof CreateDirectoryOptions)
          || !((CreateDirectoryOptions) options).isAllowExists()) {
        throw new FileAlreadyExistsException(path);
      }
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
    MutableLockedInodePath extensibleInodePath = (MutableLockedInodePath) inodePath;
    String[] pathComponents = extensibleInodePath.getPathComponents();
    String name = path.getName();

    // pathIndex is the index into pathComponents where we start filling in the path from the inode.
    int pathIndex = extensibleInodePath.size();
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
    InodeView ancestorInode = extensibleInodePath.getAncestorInode();
    if (!ancestorInode.isDirectory()) {
      throw new InvalidPathException("Could not traverse to parent directory of path " + path
          + ". Component " + pathComponents[pathIndex - 1] + " is not a directory.");
    }
    InodeDirectoryView currentInodeDirectory = (InodeDirectoryView) ancestorInode;

    List<InodeView> createdInodes = new ArrayList<>();
    List<InodeView> modifiedInodes = new ArrayList<>();
    if (options.isPersisted()) {
      // Synchronously persist directories. These inodes are already READ locked.
      for (InodeView inode : traversalResult.getNonPersisted()) {
        // This cast is safe because we've already verified that the file inode doesn't exist.
        syncPersistExistingDirectory(rpcContext, (InodeDirectoryView) inode);
      }
    }
    if ((pathIndex < (pathComponents.length - 1) || currentInodeDirectory.getChild(name) == null)
        && options.getOperationTimeMs() > currentInodeDirectory.getLastModificationTimeMs()) {
      // (1) There are components in parent paths that need to be created. Or
      // (2) The last component of the path needs to be created.
      // In these two cases, the last traversed Inode will be modified if the new timestamp is after
      // the existing last modified time.
      mState.applyAndJournal(rpcContext, UpdateInodeEntry.newBuilder()
          .setId(currentInodeDirectory.getId())
          .setLastModificationTimeMs(options.getOperationTimeMs())
          .build());
      modifiedInodes.add(currentInodeDirectory);
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
        .setGroup(options.getGroup())
        .setTtl(options.getTtl())
        .setTtlAction(options.getTtlAction());
    for (int k = pathIndex; k < (pathComponents.length - 1); k++) {
      InodeDirectoryView dir = null;
      while (dir == null) {
        InodeDirectory newDir = InodeDirectory.create(
            mDirectoryIdGenerator.getNewDirectoryId(rpcContext.getJournalContext()),
            currentInodeDirectory.getId(), pathComponents[k], missingDirOptions);
        // Lock the newly created inode before subsequent operations, and add it to the lock group.
        extensibleInodePath.getLockList().lockWriteAndCheckNameAndParent(newDir,
            currentInodeDirectory, pathComponents[k]);

        newDir.setPinned(currentInodeDirectory.isPinned());

        // if the parent has default ACL, copy that default ACL as the new directory's default
        // and access acl.
        if (!options.isMetadataLoad()) {
          DefaultAccessControlList dAcl = currentInodeDirectory.getDefaultACL();
          if (!dAcl.isEmpty()) {
            Pair<AccessControlList, DefaultAccessControlList> pair = dAcl.generateChildDirACL();
            newDir.setInternalAcl(pair.getFirst());
            newDir.setDefaultACL(pair.getSecond());
          }
        }

        if (mState.applyAndJournal(rpcContext, newDir)) {
          // After creation and journaling, downgrade to a read lock.
          extensibleInodePath.getLockList().downgradeLast();
          dir = newDir;
        } else {
          // The child directory inode already exists. Get the existing child inode.
          extensibleInodePath.getLockList().unlockLast();

          InodeView existing = currentInodeDirectory.getChildReadLock(pathComponents[k],
              extensibleInodePath.getLockList());

          if (existing == null) {
            // The competing directory could have been removed.
            continue;
          }

          if (existing.isFile()) {
            throw new FileAlreadyExistsException(String.format(
                "Directory creation for %s failed. Inode %s is a file", path, existing.getName()));
          }
          dir = (InodeDirectoryView) existing;
        }

        // Persist the directory *after* it exists in the inode tree. This prevents multiple
        // concurrent creates from trying to persist the same directory name.
        if (options.isPersisted()) {
          syncPersistExistingDirectory(rpcContext, dir);
        }
      }

      createdInodes.add(dir);
      currentInodeDirectory = dir;
    }

    // Create the final path component. First we need to make sure that there isn't already a file
    // here with that name. If there is an existing file that is a directory and we're creating a
    // directory, update persistence property of the directories if needed, otherwise, throw
    // FileAlreadyExistsException unless options.allowExists is true.
    while (true) {
      // Try to lock the last inode with the lock mode of the path.
      InodeView lastLockedInode;
      switch (extensibleInodePath.getLockMode()) {
        case READ:
          lastLockedInode = currentInodeDirectory.getChildReadLock(name,
              extensibleInodePath.getLockList());
          break;
        case WRITE_PARENT:
        case WRITE:
          lastLockedInode = currentInodeDirectory.getChildWriteLock(name,
              extensibleInodePath.getLockList());
          break;
        default:
          // This should not be reachable.
          throw new IllegalStateException(String.format("Unexpected lock mode encountered: %s",
              extensibleInodePath.getLockMode()));
      }
      if (lastLockedInode != null) {
        // inode to create already exists
        // We need to remove the last inode from the locklist because it was locked during
        // traversal and locked here again
        extensibleInodePath.getLockList().unlockLast();
        if (lastLockedInode.isDirectory() && options instanceof CreateDirectoryOptions
            && !lastLockedInode.isPersisted() && options.isPersisted()) {
          // The final path component already exists and is not persisted, so it should be added
          // to the non-persisted Inodes of traversalResult.
          syncPersistExistingDirectory(rpcContext, (InodeDirectoryView) lastLockedInode);
        } else if (!lastLockedInode.isDirectory() || !(options instanceof CreateDirectoryOptions
            && ((CreateDirectoryOptions) options).isAllowExists())) {
          String errorMessage = ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path);
          LOG.error(errorMessage);
          throw new FileAlreadyExistsException(errorMessage);
        }
        break;
      }

      Inode<?> newInode;
      // create the new inode, with a write lock
      if (options instanceof CreateDirectoryOptions) {
        CreateDirectoryOptions directoryOptions = (CreateDirectoryOptions) options;
        InodeDirectory newDir = InodeDirectory.create(
            mDirectoryIdGenerator.getNewDirectoryId(rpcContext.getJournalContext()),
            currentInodeDirectory.getId(), name, directoryOptions);

        // Lock the created inode before subsequent operations, and add it to the lock group.

        extensibleInodePath.getLockList().lockWriteAndCheckNameAndParent(newDir,
            currentInodeDirectory, name);

        // if the parent has default ACL, copy that default ACL as the new directory's default
        // and access acl.
        DefaultAccessControlList dAcl = currentInodeDirectory.getDefaultACL();
        if (!dAcl.isEmpty()) {
          Pair<AccessControlList, DefaultAccessControlList> pair = dAcl.generateChildDirACL();
          newDir.setInternalAcl(pair.getFirst());
          newDir.setDefaultACL(pair.getSecond());
        }

        if (directoryOptions.isPersisted()) {
          // Do not journal the persist entry, since a creation entry will be journaled instead.
          if (options.isMetadataLoad()) {
            // if we are creating the file as a result of loading metadata, the newDir is already
            // persisted, and we got the permissions info from the ufs.
            newDir.setOwner(options.getOwner())
                .setGroup(options.getGroup())
                .setMode(options.getMode().toShort());

            Long lastModificationTime = options.getOperationTimeMs();
            if (lastModificationTime != null) {
              newDir.setLastModificationTimeMs(lastModificationTime, true);
            }
            newDir.setPersistenceState(PersistenceState.PERSISTED);
          } else {
            syncPersistNewDirectory(newDir);
          }
        }
        newInode = newDir;
      } else if (options instanceof CreateFileOptions) {
        CreateFileOptions fileOptions = (CreateFileOptions) options;
        InodeFile newFile = InodeFile.create(mContainerIdGenerator.getNewContainerId(),
            currentInodeDirectory.getId(), name, System.currentTimeMillis(), fileOptions);
        // Lock the created inode before subsequent operations, and add it to the lock group.

        extensibleInodePath.getLockList().lockWriteAndCheckNameAndParent(newFile,
            currentInodeDirectory, name);

        // if the parent has a default ACL, copy that default ACL as the new file's access ACL.
        DefaultAccessControlList dAcl = currentInodeDirectory.getDefaultACL();
        if (!dAcl.isEmpty()) {
          AccessControlList acl = dAcl.generateChildFileACL();
          newFile.setInternalAcl(acl);
        }

        if (fileOptions.isCacheable()) {
          newFile.setCacheable(true);
        }
        newInode = newFile;
      } else {
        throw new IllegalStateException(String.format("Unrecognized create options: %s", options));
      }
      newInode.setPinned(currentInodeDirectory.isPinned());

      if (!mState.applyAndJournal(rpcContext, newInode)) {
        // Could not add the child inode to the parent. Continue and try again.
        // Cleanup is not necessary, since other state is updated later, after a successful add.
        extensibleInodePath.getLockList().unlockLast();
        continue;
      }

      createdInodes.add(newInode);
      LOG.debug("createFile: File Created: {} parent: {}", newInode, currentInodeDirectory);
      break;
    }
    return new CreatePathResult(modifiedInodes, createdInodes);
  }

  /**
   * Locks from a specific point in the tree to the descendant, and return a lockedInodePath.
   *
   * @param inodePath the root to start locking
   * @param lockMode the lock type to use
   * @param descendantUri the path to the descendant that we are locking
   * @return  an {@link InodeLockList} representing the list of descendants that got locked as
   * a result of this call.
   * @throws FileDoesNotExistException if inode does not exist
   */
  public LockedInodePath lockDescendantPath(LockedInodePath inodePath, LockMode lockMode,
      AlluxioURI descendantUri) throws InvalidPathException {
    InodeLockList descendantLockList = lockDescendant(inodePath, lockMode, descendantUri);
    return new MutableLockedInodePath(descendantUri,
        new CompositeInodeLockList(inodePath.mLockList, descendantLockList), lockMode);
  }

  /**
   * Lock from a specific poiint in the tree to the immediate child, and return a lockedInodePath.
   *
   * @param inodePath the root to start locking
   * @param lockMode the lock type to use
   * @param childInode the inode of the child that we are locking
   * @param pathComponents the array of pre-parsed path components, or null to parse pathComponents
   *                       from the uri
   * @return an {@link InodeLockList} representing the list of descendants that got locked as
   * a result of this call.
   * @throws FileDoesNotExistException if the inode does not exist
   * @throws InvalidPathException if the path is invalid
   */
  public LockedInodePath lockChildPath(LockedInodePath inodePath, LockMode lockMode,
      InodeView childInode, String[] pathComponents)
      throws FileDoesNotExistException, InvalidPathException {
    InodeLockList inodeLockList = new InodeLockList();

    if (lockMode == LockMode.READ) {
      inodeLockList.lockReadAndCheckParent(childInode, inodePath.getInode());
    } else {
      inodeLockList.lockWriteAndCheckParent(childInode, inodePath.getInode());
    }

    if (pathComponents == null) {
      return new MutableLockedInodePath(inodePath.getUri().joinUnsafe(childInode.getName()),
          new CompositeInodeLockList(inodePath.mLockList, inodeLockList), lockMode);
    } else {
      return new MutableLockedInodePath(inodePath.getUri().joinUnsafe(childInode.getName()),
          new CompositeInodeLockList(inodePath.mLockList, inodeLockList), pathComponents, lockMode);
    }
  }

  /**
   * Locks a specific descendant of a particular {@link LockedInodePath}. It does not extend the
   * {@link LockedInodePath}, it only locks the descendant.
   *
   * @param inodePath the root to start locking
   * @param lockMode the lock type to use
   * @param descendantUri the path to the descendant that we are locking
   * @return  an {@link InodeLockList} representing the list of descendants that got locked as
   * a result of this call.
   * @throws FileDoesNotExistException if inode does not exist
   */
  private InodeLockList lockDescendant(LockedInodePath inodePath, LockMode lockMode,
                                       AlluxioURI descendantUri) throws InvalidPathException {
    // Check if the descendant is really the descendant of inodePath
    if (!PathUtils.hasPrefix(descendantUri.getPath(), inodePath.getUri().getPath())
        || descendantUri.getPath().equals(inodePath.getUri().getPath())) {
      throw new InvalidPathException(descendantUri.getPath() + " is not a valid descendant of "
          + inodePath.getUri().getPath());
    }

    List<InodeView> nonPersistedInodes = new ArrayList<>();
    List<InodeView> inodeList = new ArrayList<>(inodePath.getInodeList());
    for (InodeView inode : inodeList) {
      if (!inode.isPersisted()) {
        nonPersistedInodes.add(inode);
      }
    }
    // Lock from inodePath to the descendant
    InodeLockList lockList = new InodeLockList();
    TraversalResult traversalResult = traverseToInodeInternal(
        PathUtils.getPathComponents(descendantUri.getPath()),
        inodeList, nonPersistedInodes, lockList, lockMode, null);
    if (traversalResult.mFound) {
      return traversalResult.mLockList;
    } else {
      throw new InvalidPathException(descendantUri.getPath()
          + " path not found in traversal starting from " + inodePath.getUri().getPath() + ".");
    }
  }

  /**
   * Locks all descendants of a particular {@link LockedInodePath}. Any directory inode
   * precedes its descendants in the list.
   *
   * @param inodePath the root {@link LockedInodePath} to retrieve all descendants from
   * @param lockMode the lock type to use
   * @return an {@link InodeLockList} representing the list of all descendants
   */
  public LockedInodePathList lockDescendants(LockedInodePath inodePath, LockMode lockMode) {
    List<LockedInodePath> inodePathList = new ArrayList<>();
    lockDescendantsInternal(inodePath, lockMode, inodePathList);
    return new LockedInodePathList(inodePathList);
  }

  private void lockDescendantsInternal(LockedInodePath inodePath, LockMode lockMode,
      List<LockedInodePath> inodePathList) {
    InodeView inode = inodePath.getInodeOrNull();
    if (inode == null || !inode.isDirectory()) {
      return;
    }
    InodeDirectoryView inodeDirectory = (InodeDirectoryView) inode;
    for (InodeView child : inodeDirectory.getChildren()) {
      LockedInodePath lockedDescendantPath;
      try {
        lockedDescendantPath =
            inodePath.createTempPathForExistingChild(child, inodePath.getLockMode());
        inodePathList.add(lockedDescendantPath);
      } catch (InvalidPathException | FileDoesNotExistException e) {
        // Inode is no longer a child, continue.
        continue;
      }
      if (child.isDirectory()) {
        lockDescendantsInternal(lockedDescendantPath, lockMode, inodePathList);
      }
    }
  }

  /**
   * Deletes a single inode from the inode tree by removing it from the parent inode.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to delete
   * @param opTimeMs the operation time
   * @throws FileDoesNotExistException if the Inode cannot be retrieved
   */
  public void deleteInode(RpcContext rpcContext, LockedInodePath inodePath, long opTimeMs)
      throws FileDoesNotExistException {
    InodeView inode = inodePath.getInode();
    InodeDirectoryView parent = (InodeDirectoryView) mInodes.getById(inode.getParentId());
    if (parent == null) {
      LOG.warn("Parent id not found: {} deleting inode: {}", inode.getParentId(), inode);
      throw new FileDoesNotExistException(
          ExceptionMessage.INODE_DOES_NOT_EXIST.getMessage(inode.getParentId()));
    }

    mState.applyAndJournal(rpcContext, DeleteFileEntry.newBuilder()
        .setId(inode.getId())
        .setRecursive(false)
        .setOpTimeMs(opTimeMs)
        .build());

    if (inode.isFile()) {
      rpcContext.getBlockDeletionContext()
          .registerBlocksForDeletion(((InodeFileView) inode).getBlockIds());
    }
  }

  /**
   * Sets the pinned state of an inode. If the inode is a directory, the pinned state will be set
   * recursively.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to set the pinned state for
   * @param pinned the pinned state to set for the inode (and possible descendants)
   * @param opTimeMs the operation time
   * @throws FileDoesNotExistException if inode does not exist
   */
  public void setPinned(RpcContext rpcContext, LockedInodePath inodePath, boolean pinned,
      long opTimeMs) throws FileDoesNotExistException {
    InodeView inode = inodePath.getInode();

    mState.applyAndJournal(rpcContext, UpdateInodeEntry.newBuilder()
        .setId(inode.getId())
        .setPinned(pinned)
        .setLastModificationTimeMs(opTimeMs)
        .build());

    if (inode.isDirectory()) {
      assert inode instanceof InodeDirectoryView;
      // inode is a directory. Set the pinned state for all children.
      try {
        for (InodeView child : ((InodeDirectoryView) inode).getChildren()) {
          try (LockedInodePath childPath = lockDescendantPath(inodePath, LockMode.WRITE,
              inodePath.getUri().join(child.getName()))) {
            setPinned(rpcContext, childPath, pinned, opTimeMs);
          }
        }
      } catch (InvalidPathException e) {
        LOG.warn("setPinned encountered an invalid path {}", inodePath.mUri.getPath());
      }
    }
  }

  /**
   * Sets the pinned state of an inode. If the inode is a directory, the pinned state will be set
   * recursively.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to set the pinned state for
   * @param pinned the pinned state to set for the inode (and possible descendants)
   * @throws FileDoesNotExistException if inode does not exist
   */
  public void setPinned(RpcContext rpcContext, LockedInodePath inodePath, boolean pinned)
      throws FileDoesNotExistException, InvalidPathException {
    setPinned(rpcContext, inodePath, pinned, System.currentTimeMillis());
  }

  /**
   * @return the set of file ids which are pinned
   */
  public Set<Long> getPinIdSet() {
    return new HashSet<>(mState.getPinnedInodeFileIds());
  }

  /**
   * @param fileId the file id to check
   * @return true if the given file id is the root id
   */
  public boolean isRootId(long fileId) {
    Preconditions.checkNotNull(mState.getRoot(),
        PreconditionMessage.INODE_TREE_UNINITIALIZED_IS_ROOT_ID);
    return fileId == mState.getRoot().getId();
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    // Write tree via breadth-first traversal, so that during deserialization, it may be more
    // efficient than depth-first during deserialization due to parent directory's locality.
    Queue<InodeView> inodes = new LinkedList<>();
    if (mState.getRoot() != null) {
      inodes.add(mState.getRoot());
    }
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
        InodeView inode = inodes.poll();
        if (inode.isDirectory()) {
          inodes.addAll(((InodeDirectoryView) inode).getChildren());
        }
        return inode.toJournalEntry();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove is not supported in inode tree iterator");
      }
    };
  }

  /**
   * Resets the inode tree state.
   */
  public void reset() {
    mState.reset();
  }

  /**
   * Synchronously persists an {@link InodeDirectory} to the UFS. If concurrent calls are made, only
   * one thread will persist to UFS, and the others will wait until it is persisted.
   *
   * @param context journal context supplier
   * @param dir the {@link InodeDirectory} to persist
   * @throws InvalidPathException if the path for the inode is invalid
   * @throws FileDoesNotExistException if the path for the inode is invalid
   */
  public void syncPersistExistingDirectory(Supplier<JournalContext> context, InodeDirectoryView dir)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    Preconditions.checkState(mInodes.containsId(dir.getId()));
    RetryPolicy retry =
        new ExponentialBackoffRetry(PERSIST_WAIT_BASE_SLEEP_MS, PERSIST_WAIT_MAX_SLEEP_MS,
            PERSIST_WAIT_MAX_RETRIES);
    while (retry.attempt()) {
      if (dir.getPersistenceState() == PersistenceState.PERSISTED) {
        // The directory is persisted
        return;
      }
      Optional<Scoped> persisting = dir.tryAcquirePersistingLock();
      if (!persisting.isPresent()) {
        // Someone else is doing this persist. Continue and wait for them to finish.
        continue;
      }
      try (Scoped s = persisting.get()) {
        if (dir.getPersistenceState() == PersistenceState.PERSISTED) {
          // The directory is persisted
          return;
        }
        mState.applyAndJournal(context, UpdateInodeEntry.newBuilder()
            .setId(dir.getId())
            .setPersistenceState(PersistenceState.TO_BE_PERSISTED.name())
            .build());
        UpdateInodeEntry.Builder entry = UpdateInodeEntry.newBuilder()
            .setId(dir.getId());
        syncPersistDirectory(dir).ifPresent(status -> {
          if (isRootId(dir.getId())) {
            // Don't load the root dir metadata from UFS
            return;
          }
          entry.setOwner(status.getOwner())
              .setGroup(status.getGroup())
              .setMode(status.getMode());

          Long lastModificationTime = status.getLastModifiedTime();
          if (lastModificationTime != null) {
            entry.setLastModificationTimeMs(lastModificationTime)
                .setOverwriteModificationTime(true);
          }
        });
        entry.setPersistenceState(PersistenceState.PERSISTED.name());

        mState.applyAndJournal(context, entry.build());
        return;
      }
    }
    throw new IOException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(dir.getName()));
  }

  /**
   * Synchronously persists an {@link InodeDirectory} to the UFS.
   *
   * This method does not handle concurrent modification to the given inode, so the inode must not
   * yet be added to the inode tree.
   *
   * @param dir the {@link InodeDirectory} to persist
   */
  public void syncPersistNewDirectory(InodeDirectory dir)
      throws InvalidPathException, FileDoesNotExistException, IOException {
    Preconditions.checkState(!mInodes.containsId(dir.getId()));
    dir.setPersistenceState(PersistenceState.TO_BE_PERSISTED);
    syncPersistDirectory(dir).ifPresent(status -> {
      // If the directory already exists in the UFS, update our metadata to match the UFS.
      dir.setOwner(status.getOwner())
          .setGroup(status.getGroup())
          .setMode(status.getMode());

      Long lastModificationTime = status.getLastModifiedTime();
      if (lastModificationTime != null) {
        dir.setLastModificationTimeMs(lastModificationTime, true);
      }
    });
    dir.setPersistenceState(PersistenceState.PERSISTED);
  }

  /**
   * Persists the directory to the UFS, returning the UFS status if the directory is found to
   * already exist in the UFS.
   *
   * @param dir the directory to persist
   * @return optional ufs status if the directory already existed
   */
  private Optional<UfsStatus> syncPersistDirectory(InodeDirectoryView dir)
      throws FileDoesNotExistException, IOException, InvalidPathException {
    AlluxioURI uri = getPath(dir);
    MountTable.Resolution resolution = mMountTable.resolve(uri);
    String ufsUri = resolution.getUri().toString();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      MkdirsOptions mkdirsOptions = MkdirsOptions.defaults().setCreateParent(false)
          .setOwner(dir.getOwner()).setGroup(dir.getGroup()).setMode(new Mode(dir.getMode()));
      if (!ufs.mkdirs(ufsUri, mkdirsOptions)) {
        // Directory might already exist. Try loading the status from ufs.
        UfsStatus status;
        try {
          status = ufs.getStatus(ufsUri);
        } catch (Exception e) {
          throw new IOException(String.format("Cannot create or load UFS directory %s: %s.",
              ufsUri, e.toString()), e);
        }
        if (status.isFile()) {
          throw new InvalidPathException(String.format(
              "Error persisting directory. A file exists at the UFS location %s.", ufsUri));
        }
        return Optional.of(status);
      }
    }
    return Optional.empty();
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
    List<InodeView> nonPersistedInodes = new ArrayList<>();
    List<InodeView> inodes = new ArrayList<>();
    InodeLockList lockList = new InodeLockList();
    InodeDirectoryView root = mState.getRoot();

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
            lockList.lockRead(root);
          } else {
            lockList.lockWrite(root);
          }
          inodes.add(root);
          valid = true;
          return TraversalResult.createFoundResult(nonPersistedInodes, inodes, lockList);
        } else {
          throw new InvalidPathException(
              ExceptionMessage.PATH_COMPONENTS_INVALID_START.getMessage(pathComponents[0]));
        }
      }

      if (getLockModeForComponent(0, pathComponents.length, lockMode, lockHints) == LockMode.READ) {
        lockList.lockRead(root);
      } else {
        lockList.lockWrite(root);
      }
      inodes.add(root);
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
    List<InodeView> inodes = extensibleInodePath.getInodeList();
    InodeLockList lockList = extensibleInodePath.getLockList();
    List<InodeView> nonPersistedInodes = new ArrayList<>();
    for (InodeView inode : inodes) {
      if (!inode.isPersisted()) {
        nonPersistedInodes.add(inode);
      }
    }
    return traverseToInodeInternal(extensibleInodePath.getPathComponents(), inodes,
        nonPersistedInodes, lockList, lockMode, null);
  }

  /**
   * Traverse the tree to find the rest of the given {@link LockedInodePath}. Hints for the lock
   * mode at each path.
   *
   * @param pathComponents components of the path that are are traversing towards
   * @param inodes inodes that are already locked and will no longer require locking,
   *               modified in the method to return all inode along the path
   * @param nonPersistedInodes nonPersistedInodes, modified in the method to return
   *                           all nonPersistedInodes
   * @param lockList lockList containing all locked inodes, modified in the method
   * @param lockMode the {@link LockMode} for the path
   * @param lockHints list of lock hints for locking the path, must be null or its size must match
   *                  the size of pathComponents
   * @return the result of the traversal
   * @throws InvalidPathException
   */
  private TraversalResult traverseToInodeInternal(String[] pathComponents, List<InodeView> inodes,
      List<InodeView> nonPersistedInodes, InodeLockList lockList, LockMode lockMode,
      List<LockMode> lockHints)
      throws InvalidPathException {
    InodeView current = inodes.get(inodes.size() - 1);
    for (int i = inodes.size(); i < pathComponents.length; i++) {
      InodeView next = ((InodeDirectoryView) current).getChild(pathComponents[i]);
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
    private final List<InodeView> mNonPersisted;

    /** The list of all inodes encountered during the traversal. */
    private final List<InodeView> mInodes;

    /** The {@link InodeLockList} managing the locks for the inodes. */
    private final InodeLockList mLockList;

    // TODO(gpang): consider a builder paradigm to iteratively build the traversal result.
    static TraversalResult createFoundResult(List<InodeView> nonPersisted, List<InodeView> inodes,
        InodeLockList lockList) {
      return new TraversalResult(true, nonPersisted, inodes, lockList);
    }

    static TraversalResult createNotFoundResult(int index, List<InodeView> nonPersisted,
        List<InodeView> inodes, InodeLockList lockList) {
      return new TraversalResult(false, nonPersisted, inodes, lockList);
    }

    private TraversalResult(boolean found, List<InodeView> nonPersisted,
        List<InodeView> inodes, InodeLockList lockList) {
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
    List<InodeView> getNonPersisted() {
      return mNonPersisted;
    }

    /**
     * @return the list of all inodes encountered during the traversal
     */
    List<InodeView> getInodes() {
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
    private final List<InodeView> mModified;
    private final List<InodeView> mCreated;

    /**
     * Constructs the results of modified and created inodes when creating a path.
     *
     * @param modified a list of modified inodes
     * @param created a list of created inodes
     */
    CreatePathResult(List<InodeView> modified, List<InodeView> created) {
      mModified = Preconditions.checkNotNull(modified, "modified");
      mCreated = Preconditions.checkNotNull(created, "created");
    }

    /**
     * @return the list of inodes modified during path creation
     */
    public List<InodeView> getModified() {
      return mModified;
    }

    /**
     * @return the list of inodes created during path creation
     */
    public List<InodeView> getCreated() {
      return mCreated;
    }
  }
}
