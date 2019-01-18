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
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.master.block.ContainerIdGenerable;
import alluxio.master.file.RpcContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.CreatePathContext;
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
import alluxio.resource.LockResource;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.interfaces.Scoped;

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
   * Patterns of inode path locking.
   */
  public enum LockPattern {
    /**
     * Read lock every existing inode and edge along the path. Useful when we want to read an inode
     * without modifying anything.
     *
     * Examples
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b
     * result: Read locks on [a, a->b, b]
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b/c
     * result: Read locks on [a, a->b, b, b->c, c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a
     * result: Read locks on [a]
     */
    READ,
    /**
     * Read lock every existing inode and edge along the path, but write lock the final inode if it
     * exists. If the inode does not exist, read lock the edge leading out of the final existing
     * ancestor. Useful when we want to modify an inode's metadata without changing the structure
     * of the inode tree (no create/rename/delete).
     *
     * Examples
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b
     * result: Read locks on [a, a->b, b, b->c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b/c
     * result: Read locks on [a, a->b, b, b->c], Write locks on [c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a
     * result: Read locks on [a, a->b]
     */
    WRITE_INODE,
    /**
     * Read lock every existing inode and edge along the path, but write lock the edge leading out
     * of the last existing ancestor. Useful when we want to modify the structure of the inode tree,
     * e.g. when creating, deleting, or renaming inodes.
     *
     * Examples
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b
     * result: Read locks on [a, a->b, b], Write locks on [b->c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b/c
     * result: Read locks on [a, a->b, b], Write locks on [b->c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a
     * result: Read locks on [a], Write locks [a->b]
     */
    WRITE_EDGE;

    /**
     * @return whether the lock pattern is one of the write-type patterns
     */
    public boolean isWrite() {
      return this == WRITE_INODE || this == WRITE_EDGE;
    }
  }

  /**
   * ReadWriteLock types.
   */
  public enum LockMode {
    READ,
    WRITE;
  }

  /** Only the root inode should have the empty string as its name. */
  public static final String ROOT_INODE_NAME = "";

  /** Number of retries when trying to lock a path, from a given id. */
  public static final int PATH_TRAVERSAL_RETRIES = 1000;

  /** Mount table manages the file system mount points. */
  private final MountTable mMountTable;

  private final TtlBucketList mTtlBuckets;

  /** Manager for inode locking. */
  private final InodeLockManager mInodeLockManager;

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
    mInodeLockManager = new InodeLockManager();
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
          CreateDirectoryContext
              .defaults(CreateDirectoryPOptions.newBuilder().setMode(mode.toProto()))
              .setOwner(owner).setGroup(group));
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
   * Locks existing inodes on the specified path, in the specified {@link LockPattern}. The target
   * inode is not required to exist.
   *
   * @param uri the uri to lock
   * @param lockPattern the {@link LockPattern} to lock the inodes with
   * @return the {@link LockedInodePath} representing the locked path of inodes
   * @throws InvalidPathException if the path is invalid
   */
  public LockedInodePath lockInodePath(AlluxioURI uri, LockPattern lockPattern)
      throws InvalidPathException {
    LockedInodePath inodePath = new LockedInodePath(uri, mInodeLockManager, getRoot(), lockPattern);
    try {
      inodePath.traverse();
    } catch (Throwable t) {
      inodePath.close();
      throw t;
    }
    return inodePath;
  }

  /**
   * @param uri the {@link AlluxioURI} to check for existence
   * @return whether the inode exists
   */
  public boolean inodePathExists(AlluxioURI uri) {
    try (LockedInodePath inodePath = lockInodePath(uri, LockPattern.READ)) {
      return inodePath.fullPathExists();
    } catch (InvalidPathException e) {
      return false;
    }
  }

  /**
   * Locks a path and throws an exception if the path does not exist.
   *
   * @param uri a uri to lock
   * @param lockPattern the pattern to lock with
   * @return a locked inode path for the uri
   */
  public LockedInodePath lockFullInodePath(AlluxioURI uri, LockPattern lockPattern)
      throws InvalidPathException, FileDoesNotExistException {
    LockedInodePath inodePath = lockInodePath(uri, lockPattern);
    if (!inodePath.fullPathExists()) {
      inodePath.close();
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(uri));
    }
    return inodePath;
  }

  /**
   * Locks a path and throws an exception if the path does not exist.
   *
   * @param id the inode id to lock
   * @param lockPattern the pattern to lock with
   * @return a locked inode path for the uri
   */
  public LockedInodePath lockFullInodePath(long id, LockPattern lockPattern)
      throws FileDoesNotExistException {
    LockedInodePath inodePath = lockInodePathById(id, lockPattern);
    if (!inodePath.fullPathExists()) {
      inodePath.close();
      throw new FileDoesNotExistException(ExceptionMessage.INODE_DOES_NOT_EXIST.getMessage(id));
    }
    return inodePath;
  }

  /**
   * Locks existing inodes on the path to the inode specified by an id, in the specified
   * {@link LockPattern}. The target inode must exist. This may require multiple traversals of the
   * tree, so may be inefficient.
   *
   * @param id the inode id
   * @param lockPattern the {@link LockPattern} to lock the inodes with
   * @return the {@link LockedInodePath} representing the locked path of inodes
   * @throws FileDoesNotExistException if the target inode does not exist
   */
  private LockedInodePath lockInodePathById(long id, LockPattern lockPattern)
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
        inodePath = lockInodePath(uri, lockPattern);
        if (inodePath.getInode().getId() == id) {
          // Set to true, so the path is not unlocked before returning.
          valid = true;
          return inodePath;
        }
        // The path does not end up at the target inode id. Repeat the traversal.
      } catch (InvalidPathException e) {
        // ignore and repeat the loop
        LOG.debug("Inode lookup id {} computed path {} mismatch id. Repeating.", id, uri);
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
   * Locks existing inodes on the two specified paths. The two paths will be locked in the correct
   * order. The target inodes are not required to exist.
   *
   * @param path1 the first path to lock
   * @param lockPattern1 the locking pattern for the first path
   * @param path2 the second path to lock
   * @param lockPattern2 the locking pattern for the second path
   * @return a {@link InodePathPair} representing the two locked paths
   * @throws InvalidPathException if a path is invalid
   */
  public InodePathPair lockInodePathPair(AlluxioURI path1, LockPattern lockPattern1,
      AlluxioURI path2, LockPattern lockPattern2) throws InvalidPathException {
    LockedInodePath lockedPath1 = null;
    LockedInodePath lockedPath2 = null;
    boolean valid = false;
    try {
      // Lock paths in a deterministic order.
      if (path1.getPath().compareTo(path2.getPath()) > 0) {
        lockedPath2 = lockInodePath(path2, lockPattern2);
        lockedPath1 = lockInodePath(path1, lockPattern1);
      } else {
        lockedPath1 = lockInodePath(path1, lockPattern1);
        lockedPath2 = lockInodePath(path2, lockPattern2);
      }
      valid = true;
      return new InodePathPair(lockedPath1, lockedPath2);
    } finally {
      if (!valid) {
        if (lockedPath1 != null) {
          lockedPath1.close();
        }
        if (lockedPath2 != null) {
          lockedPath2.close();
        }
      }
    }
  }

  /**
   * Attempts to extend an existing {@link LockedInodePath} to reach the target inode (the last
   * inode for the full path). If the target inode does not exist, an exception will be thrown.
   *
   * @param inodePath the {@link LockedInodePath} to extend to the target inode
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the target inode does not exist
   */
  public void ensureFullInodePath(LockedInodePath inodePath)
      throws InvalidPathException, FileDoesNotExistException {
    if (inodePath.fullPathExists()) {
      return;
    }
    inodePath.traverse();
    if (!inodePath.fullPathExists()) {
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
    long id;
    long parentId;
    String name;
    try (LockResource lr = mInodeLockManager.lockInode(inode, LockMode.READ)) {
      id = inode.getId();
      parentId = inode.getParentId();
      name = inode.getName();
    }

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
   * This method expects that the last edge leading to the target inode to be write-locked. If the
   * last existing inode in the path is /a/b/c and we want to create /a/b/c/d/e, the c->d edge must
   * be write locked.
   *
   * On success, createPath attempts to push the write lock forward as far as possible. For the
   * above example, createPath would take a write lock on d->e, and downgrade the c->d lock from a
   * write lock to a read lock. This may not be possible if inodePath is a composite path which
   * doesn't own the write lock. In that case no downgrade will occur.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path
   * @param context method context
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
      CreatePathContext<?, ?> context) throws FileAlreadyExistsException, BlockInfoException,
      InvalidPathException, IOException, FileDoesNotExistException {
    Preconditions.checkState(inodePath.getLockPattern() == LockPattern.WRITE_EDGE);

    // TODO(gpang): consider splitting this into createFilePath and createDirectoryPath, with a
    // helper method for the shared logic.
    AlluxioURI path = inodePath.getUri();
    if (path.isRoot()) {
      String errorMessage = ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path);
      LOG.error(errorMessage);
      throw new FileAlreadyExistsException(errorMessage);
    }
    if (inodePath.fullPathExists()) {
      if (context instanceof CreateDirectoryContext
          && ((CreateDirectoryContext) context).getOptions().getAllowExists()) {
        return new CreatePathResult(new ArrayList<>(), new ArrayList<>());
      } else {
        throw new FileAlreadyExistsException(path);
      }
    }

    if (context instanceof CreateFileContext) {
      CreateFileContext fileContext = (CreateFileContext) context;
      if (fileContext.getOptions().getBlockSizeBytes() < 1) {
        throw new BlockInfoException(
            "Invalid block size " + fileContext.getOptions().getBlockSizeBytes());
      }
    }

    LOG.debug("createPath {}", path);

    String[] pathComponents = inodePath.mPathComponents;
    String name = path.getName();

    // pathIndex is the index into pathComponents where we start filling in the path from the inode.
    int pathIndex = inodePath.getExistingInodeCount();
    if (pathIndex < pathComponents.length - 1) {
      // The immediate parent was not found. If it's not recursive, we throw an exception here.
      // Otherwise we add the remaining path components to the list of components to create.
      if (!context.isRecursive()) {
        throw new FileDoesNotExistException(String.format(
            "File %s creation failed. Component %d(%s) does not exist",
            path, pathIndex, pathComponents[pathIndex]));
      }
    }
    // The ancestor inode (parent or ancestor) of the target path.
    InodeView ancestorInode = inodePath.getAncestorInode();
    if (!ancestorInode.isDirectory()) {
      throw new InvalidPathException("Could not traverse to parent directory of path " + path
          + ". Component " + pathComponents[pathIndex - 1] + " is not a directory.");
    }
    InodeDirectoryView currentInodeDirectory = (InodeDirectoryView) ancestorInode;

    List<InodeView> createdInodes = new ArrayList<>();
    List<InodeView> modifiedInodes = new ArrayList<>();
    if (context.isPersisted()) {
      // Synchronously persist directories. These inodes are already READ locked.
      for (InodeView inode : inodePath.getInodeList()) {
        if (!inode.isPersisted()) {
          // This cast is safe because we've already verified that the file inode doesn't exist.
          syncPersistExistingDirectory(rpcContext, (InodeDirectoryView) inode);
        }
      }
    }
    if ((pathIndex < (pathComponents.length - 1) || currentInodeDirectory.getChild(name) == null)
        && context.getOperationTimeMs() > currentInodeDirectory.getLastModificationTimeMs()) {
      // (1) There are components in parent paths that need to be created. Or
      // (2) The last component of the path needs to be created.
      // In these two cases, the last traversed Inode will be modified if the new timestamp is after
      // the existing last modified time.
      mState.applyAndJournal(rpcContext, UpdateInodeEntry.newBuilder()
          .setId(currentInodeDirectory.getId())
          .setLastModificationTimeMs(context.getOperationTimeMs())
          .build());
      modifiedInodes.add(currentInodeDirectory);
    }

    // Fill in the ancestor directories that were missing.
    // NOTE, we set the mode of missing ancestor directories to be the default value, rather
    // than inheriting the option of the final file to create, because it may not have
    // "execute" permission.
    CreateDirectoryContext missingDirContext = CreateDirectoryContext.defaults();
    missingDirContext.getOptions().setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
        .setTtl(context.getTtl()).setTtlAction(context.getTtlAction()));
    missingDirContext.setPersisted(context.isPersisted());
    missingDirContext.setOperationTimeMs(context.getOperationTimeMs());
    missingDirContext.setMountPoint(false);
    missingDirContext.setOwner(context.getOwner());
    missingDirContext.setGroup(context.getGroup());
    for (int k = pathIndex; k < (pathComponents.length - 1); k++) {
      InodeDirectory newDir = InodeDirectory.create(
          mDirectoryIdGenerator.getNewDirectoryId(rpcContext.getJournalContext()),
          currentInodeDirectory.getId(), pathComponents[k], missingDirContext);

      newDir.setPinned(currentInodeDirectory.isPinned());

      // if the parent has default ACL, copy that default ACL as the new directory's default
      // and access acl, ANDed with the umask
      // if it is part of a metadata load operation, we ignore the umask and simply inherit
      // the default ACL as the directory's new default and access ACL
      short mode = context.isMetadataLoad() ? Mode.createFullAccess().toShort()
          : newDir.getMode();
      DefaultAccessControlList dAcl = currentInodeDirectory.getDefaultACL();
      if (!dAcl.isEmpty()) {
        Pair<AccessControlList, DefaultAccessControlList> pair =
            dAcl.generateChildDirACL(mode);
        newDir.setInternalAcl(pair.getFirst());
        newDir.setDefaultACL(pair.getSecond());
      }
      mState.applyAndJournal(rpcContext, newDir);

      inodePath.addNextInode(newDir);

      // Persist the directory *after* it exists in the inode tree. This prevents multiple
      // concurrent creates from trying to persist the same directory name.
      if (context.isPersisted()) {
        syncPersistExistingDirectory(rpcContext, newDir);
      }

      createdInodes.add(newDir);
      currentInodeDirectory = newDir;
    }

    // Create the final path component.
    Inode<?> newInode;
    // create the new inode, with a write lock
    if (context instanceof CreateDirectoryContext) {
      CreateDirectoryContext directoryContext = (CreateDirectoryContext) context;
      InodeDirectory newDir = InodeDirectory.create(
          mDirectoryIdGenerator.getNewDirectoryId(rpcContext.getJournalContext()),
          currentInodeDirectory.getId(), name, directoryContext);

      // if the parent has default ACL, take the default ACL ANDed with the umask as the new
      // directory's default and access acl
      // When it is a metadata load operation, do not take the umask into account
      short mode = context.isMetadataLoad() ? Mode.createFullAccess().toShort()
          : newDir.getMode();
      DefaultAccessControlList dAcl = currentInodeDirectory.getDefaultACL();
      if (!dAcl.isEmpty()) {
        Pair<AccessControlList, DefaultAccessControlList> pair =
            dAcl.generateChildDirACL(mode);
        newDir.setInternalAcl(pair.getFirst());
        newDir.setDefaultACL(pair.getSecond());
      }

      if (directoryContext.isPersisted()) {
        // Do not journal the persist entry, since a creation entry will be journaled instead.
        if (context.isMetadataLoad()) {
          // if we are creating the file as a result of loading metadata, the newDir is already
          // persisted, and we got the permissions info from the ufs.
          newDir.setOwner(context.getOwner())
              .setGroup(context.getGroup())
              .setMode(context.getMode().toShort());

          Long lastModificationTime = context.getOperationTimeMs();
          if (lastModificationTime != null) {
            newDir.setLastModificationTimeMs(lastModificationTime, true);
          }
          newDir.setPersistenceState(PersistenceState.PERSISTED);
        } else {
          syncPersistNewDirectory(newDir);
        }
      }
      newInode = newDir;
    } else if (context instanceof CreateFileContext) {
      CreateFileContext fileContext = (CreateFileContext) context;
      InodeFile newFile = InodeFile.create(mContainerIdGenerator.getNewContainerId(),
          currentInodeDirectory.getId(), name, System.currentTimeMillis(), fileContext);

      // if the parent has a default ACL, copy that default ACL ANDed with the umask as the new
      // file's access ACL.
      // If it is a metadata load operation, do not consider the umask.
      DefaultAccessControlList dAcl = currentInodeDirectory.getDefaultACL();
      short mode = context.isMetadataLoad() ? Mode.createFullAccess().toShort() : newFile.getMode();
      if (!dAcl.isEmpty()) {
        AccessControlList acl = dAcl.generateChildFileACL(mode);
        newFile.setInternalAcl(acl);
      }

      if (fileContext.isCacheable()) {
        newFile.setCacheable(true);
      }
      newInode = newFile;
    } else {
      throw new IllegalStateException(String.format("Unrecognized create options: %s", context));
    }
    newInode.setPinned(currentInodeDirectory.isPinned());

    mState.applyAndJournal(rpcContext, newInode);
    inodePath.addNextInode(newInode);

    createdInodes.add(newInode);
    LOG.debug("createFile: File Created: {} parent: {}", newInode, currentInodeDirectory);
    return new CreatePathResult(modifiedInodes, createdInodes);
  }

  /**
   * Returns {@link LockedInodePath}s for all descendants of inodePath.
   *
   * The descendents are implicitly locked because the current thread holds a write lock on
   * inodePath, so no new locks are acquired in this method.
   *
   * @param inodePath a locked inode path. It must be write-locked
   * @return all descendants
   */
  public List<LockedInodePath> getImplicitlyLockedDescendants(LockedInodePath inodePath) {
    Preconditions.checkState(inodePath.getLockPattern() == LockPattern.WRITE_EDGE
        || (inodePath.getLockPattern() == LockPattern.WRITE_INODE && inodePath.fullPathExists()));

    List<LockedInodePath> descendants = new ArrayList<>();
    gatherDescendants(inodePath, descendants);
    return descendants;
  }

  private void gatherDescendants(LockedInodePath inodePath, List<LockedInodePath> descendants) {
    InodeView inode = inodePath.getInodeOrNull();
    if (inode == null || inode.isFile()) {
      return;
    }
    InodeDirectoryView dir = (InodeDirectoryView) inode;
    for (InodeView child : dir.getChildren()) {
      LockedInodePath childPath = null;
      try {
        childPath = inodePath.lockChild(child, LockPattern.WRITE_EDGE);
      } catch (InvalidPathException e) {
        // Child does not exist.
        continue;
      }
      descendants.add(childPath);
      gatherDescendants(childPath, descendants);
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
      long opTimeMs) throws FileDoesNotExistException, InvalidPathException {
    Preconditions.checkState(inodePath.getLockPattern().isWrite());

    InodeView inode = inodePath.getInode();

    mState.applyAndJournal(rpcContext, UpdateInodeEntry.newBuilder()
        .setId(inode.getId())
        .setPinned(pinned)
        .setLastModificationTimeMs(opTimeMs)
        .build());

    if (inode.isDirectory()) {
      assert inode instanceof InodeDirectoryView;
      // inode is a directory. Set the pinned state for all children.
      for (InodeView child : ((InodeDirectoryView) inode).getChildren()) {
        try (LockedInodePath childPath =
            inodePath.lockChild(child, LockPattern.WRITE_INODE)) {
          // No need for additional locking since the parent is write-locked.
          setPinned(rpcContext, childPath, pinned, opTimeMs);
        }
      }
    }
  }

  /**
   * Sets the min and/or max replication level of an inode. If the inode is a directory, the state
   * will be set recursively. Arguments replicationMax and replicationMin can be null if they are
   * not meant to be set.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to set the pinned state for
   * @param replicationMax the max replication level to set for the inode (and possible descendants)
   * @param replicationMin the min replication level to set for the inode (and possible descendants)
   * @param opTimeMs the operation time
   * @throws FileDoesNotExistException if inode does not exist
   */
  public void setReplication(RpcContext rpcContext, LockedInodePath inodePath,
      Integer replicationMax, Integer replicationMin, long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException {
    Preconditions.checkArgument(replicationMin != null || replicationMax != null,
        PreconditionMessage.INVALID_REPLICATION_MAX_MIN_VALUE_NULL);
    Preconditions.checkArgument(replicationMin == null || replicationMin >= 0,
        PreconditionMessage.INVALID_REPLICATION_MIN_VALUE);
    Preconditions.checkState(inodePath.getLockPattern().isWrite());

    InodeView inode = inodePath.getInode();

    if (inode.isFile()) {
      InodeFileView inodeFile = (InodeFileView) inode;
      int newMax = (replicationMax == null) ? inodeFile.getReplicationMax() : replicationMax;
      int newMin = (replicationMin == null) ? inodeFile.getReplicationMin() : replicationMin;

      Preconditions.checkArgument(newMax == alluxio.Constants.REPLICATION_MAX_INFINITY
          || newMax >= newMin,
          PreconditionMessage.INVALID_REPLICATION_MAX_SMALLER_THAN_MIN.toString(),
          replicationMax, replicationMax);

      mState.applyAndJournal(rpcContext, UpdateInodeFileEntry.newBuilder()
          .setId(inode.getId())
          .setReplicationMax(newMax)
          .setReplicationMin(newMin)
          .build());
      mState.applyAndJournal(rpcContext, UpdateInodeEntry.newBuilder()
          .setId(inode.getId())
          .setPinned(newMin > 0)
          .setLastModificationTimeMs(opTimeMs)
          .build());
    } else {
      for (InodeView child : inode.asDirectory().getChildren()) {
        try (LockedInodePath tempInodePath =
            inodePath.lockChild(child, LockPattern.WRITE_INODE)) {
          // No need for additional locking since the parent is write-locked.
          setReplication(rpcContext, tempInodePath, replicationMax, replicationMin, opTimeMs);
        }
      }
    }
  }

  /**
   * @return the set of file ids whose replication max is not infinity
   */
  public Set<Long> getReplicationLimitedFileIds() {
    return java.util.Collections.unmodifiableSet(mState.getReplicationLimitedFileIds());
  }

  /**
   * @return the set of file ids which are pinned
   */
  public Set<Long> getPinIdSet() {
    return new HashSet<>(mState.getPinnedInodeFileIds());
  }

  /**
   * @return the inode lock manager for the inode tree
   */
  public InodeLockManager getInodeLockManager() {
    return mInodeLockManager;
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
      Optional<Scoped> persisting = mInodeLockManager.tryAcquirePersistingLock(dir.getId());
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
