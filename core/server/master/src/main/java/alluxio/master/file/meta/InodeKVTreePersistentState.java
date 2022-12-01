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

import static alluxio.conf.PropertyKey.MASTER_FILE_SYSTEM_OPERATION_RETRY_CACHE_ENABLED;
import static alluxio.conf.PropertyKey.MASTER_FILE_SYSTEM_OPERATION_RETRY_CACHE_SIZE;
import static alluxio.conf.PropertyKey.MASTER_METRICS_FILE_SIZE_DISTRIBUTION_BUCKETS;

import alluxio.conf.Configuration;
import alluxio.grpc.SetAclAction;
import alluxio.master.file.RpcContext;
import alluxio.master.journal.JournalContext;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.kvstorecaching.KVCachingInodeStore;
import alluxio.proto.journal.File.AsyncPersistRequestEntry;
import alluxio.proto.journal.File.CompleteFileEntry;
import alluxio.proto.journal.File.DeleteFileEntry;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.File.InodeLastModificationTimeEntry;
import alluxio.proto.journal.File.PersistDirectoryEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAttributeEntry;
import alluxio.proto.journal.File.UpdateInodeDirectoryEntry;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.File.UpdateInodeEntry.Builder;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;
import alluxio.resource.LockResource;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.util.BucketCounter;
import alluxio.util.FormatUtils;
import alluxio.wire.OperationId;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Class for managing persistent inode tree state.
 *
 * This class owns all persistent inode tree state, and all inode tree modifications must go through
 * this class. To modify the inode tree, create a journal entry and call one of the applyAndJournal
 * methods.
 */
public class InodeKVTreePersistentState {
  private static final Logger LOG = LoggerFactory.getLogger(InodeKVTreePersistentState.class);

  private final KVCachingInodeStore mInodeStore;
  private final InodeLockManager mInodeLockManager;

  private final boolean mRetryCacheEnabled;
  /**
   * This cache is used by this persistent state and InodeTree
   * for keeping track of recently applied operations on inodes.
   */
  private final Cache<OperationId, Boolean> mOpIdCache;

  /**
   * A set of inode ids representing pinned inode files. These are not part of the journaled state,
   * but this class keeps the set of pinned inodes up to date whenever the inode tree is modified.
   *
   * This class owns this set, and no other class can modify the set.
   */
  private final PinnedInodeFileIds mPinnedInodeFileIds = new PinnedInodeFileIds();

  /** A set of inode ids whose replication max value is non-default. */
  private final ReplicationLimitedFileIds mReplicationLimitedFileIds =
      new ReplicationLimitedFileIds();

  /** Counter for tracking how many inodes we have. */
  private final InodeCounter mInodeCounter = new InodeCounter();

  /** A set of inode ids whose persistence state is {@link PersistenceState#TO_BE_PERSISTED}. */
  private final ToBePersistedFileIds mToBePersistedIds = new ToBePersistedFileIds();

  /**
   * TTL bucket list. The list is owned by InodeTree, and is only shared with
   * InodeTreePersistentState so that the list can be updated whenever inode tree state changes.
   */
  // TODO(andrew): Move ownership of the ttl bucket list to this class
  private final TtlBucketList mTtlBuckets;

  private final BucketCounter mBucketCounter;

  /**
   * @param inodeStore file store which holds inode metadata
   * @param lockManager manager for inode locks
   * @param ttlBucketList reference to the ttl bucket list so that the list can be updated when the
   *        inode tree is modified
   */
  public InodeKVTreePersistentState(InodeStore inodeStore, InodeLockManager lockManager,
      TtlBucketList ttlBucketList) {
    mInodeStore = (KVCachingInodeStore)inodeStore;
    mInodeLockManager = lockManager;
    mTtlBuckets = ttlBucketList;
    mBucketCounter = new BucketCounter(
        Configuration.getList(MASTER_METRICS_FILE_SIZE_DISTRIBUTION_BUCKETS)
            .stream().map(FormatUtils::parseSpaceSize).collect(Collectors.toList()));
    mRetryCacheEnabled =
        Configuration.getBoolean(MASTER_FILE_SYSTEM_OPERATION_RETRY_CACHE_ENABLED);
    mOpIdCache = CacheBuilder.newBuilder()
        .maximumSize(Configuration.getInt(MASTER_FILE_SYSTEM_OPERATION_RETRY_CACHE_SIZE))
        .build();
  }

  /**
   * Whether given operation is still cached in retry-cache.
   *
   * @param opId the operation id
   * @return {@code true} if given op is marked complete
   */
  public boolean isOperationComplete(@Nullable OperationId opId) {
    return mRetryCacheEnabled && opId != null && (null != mOpIdCache.getIfPresent(opId));
  }

  /**
   * Used to mark an operation as complete in retry-cache.
   * @param opId the operation id
   */
  public void cacheOperation(@Nullable OperationId opId) {
    if (mRetryCacheEnabled && opId != null) {
      mOpIdCache.put(opId, true);
    }
  }

  /**
   * @return an unmodifiable view of the replication limited file ids
   */
  public Set<Long> getReplicationLimitedFileIds() {
    return Collections.unmodifiableSet(mReplicationLimitedFileIds);
  }

  /**
   * @return the root of the inode tree
   */
  public InodeDirectory getRoot() {
    return mInodeStore.get(0, "").map(Inode::asDirectory).orElse(null);
  }

  /**
   * @return the pinned inode file ids;
   */
  public Set<Long> getPinnedInodeFileIds() {
    return Collections.unmodifiableSet(mPinnedInodeFileIds);
  }

  /**
   * @return the number of inodes in the tree
   */
  public long getInodeCount() {
    return mInodeCounter.longValue();
  }

  /**
   * @return the file size distribution in the tree
   */
  public Map<Long, Number> getFileSizeHistogram() {
    return mBucketCounter.getCounters();
  }

  /**
   * @return an unmodifiable view of the files with persistence state
   *         {@link PersistenceState#TO_BE_PERSISTED}
   */
  public Set<Long> getToBePersistedIds() {
    // TODO(yyong) it is supposed to read from the db
    return Collections.unmodifiableSet(mToBePersistedIds);
  }

  /**
   * Deletes an inode (may be either a file or directory).
   *
   * @param context journal context supplier
   * @param entry delete file entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, DeleteFileEntry entry) {
    // Unlike most entries, the delete file entry must be applied *before* making the in-memory
    // change. This is because delete file and create file are performed with only a read lock on
    // the parent directory. As soon as we do the in-memory-delete, another thread could re-create a
    // directory with the same name, and append a journal entry for the inode creation. This would
    // ruin journal replay because we would see two create file entries in a row for the same file
    // name. The opposite order is safe. We will never append the delete entry for a file before its
    // creation entry because delete requires a write lock on the deleted file, but the create
    // operation holds that lock until after it has appended to the journal.
    delete(entry);
  }

  public long newBlock(Supplier<JournalContext> context, long parentId, String name) {
    MutableInodeFile inode = ((KVCachingInodeStore)mInodeStore).getMutable(parentId, name).get().asFile();
    long newBlockId = inode.getNewBlockId();
    mInodeStore.writeInode(inode);
    return newBlockId;
  }

  /**
   * Renames an inode.
   *
   * @param context journal context supplier
   * @param entry rename entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, RenameEntry entry) {
    applyRename(entry);
  }

  /**
   * Updates an inode's state. This is used for state common to both files and directories.
   *
   * @param entry update inode entry
   */
  public void updateInodeEntry(UpdateInodeEntry entry) {
    applyUpdateInode(entry);
  }

  /**
   * Updates an inode file's state.
   *
   * @param entry update inode file entry
   */
  public void applyAndJournal(UpdateInodeFileEntry entry) {
    applyUpdateInodeFile(entry);
  }

  /**
   * Adds an inode to the inode tree.
   *
   * @param context journal context supplier
   * @param inode an inode to add and create a journal entry for
   * @param path path of the new inode
   */
  public void applyAndJournal(Supplier<JournalContext> context, MutableInode<?> inode,
      String path) {
    createInode(inode, path);
  }


  public UpdateInodeEntry applyInodeAccessTime(long parentId, String name, long accessTime) {
    UpdateInodeEntry entry = UpdateInodeEntry.newBuilder().setParentId(parentId)
        .setName(name)
        .setLastAccessTimeMs(accessTime).build();
    applyUpdateInode(entry);
    return entry;
  }

  ////
  /// Apply Implementations. These methods are used for journal replay, so they are not allowed to
  /// fail. They are also used when making metadata changes during regular operation.
  ////

  private void delete(DeleteFileEntry entry) {
    long id = entry.getId();
    Path path = Paths.get(entry.getPath());
    long parentId = getIdFromPath(path.getParent());
    String pathName = path.getFileName().toString();

    Inode inode = mInodeStore.getChild(parentId, pathName).get();

    // The recursive option is only used by old versions.
    if (inode.isDirectory() && entry.getRecursive()) {
      Queue<InodeDirectory> dirsToDelete = new ArrayDeque<>();
      dirsToDelete.add(inode.asDirectory());
      while (!dirsToDelete.isEmpty()) {
        InodeDirectory dir = dirsToDelete.poll();
        mInodeStore.removeInodeAndParentEdge(inode);
        mInodeCounter.decrement();
        try (CloseableIterator<? extends Inode> it = mInodeStore.getChildren(dir)) {
          while (it.hasNext()) {
            Inode child = it.next();
            if (child.isDirectory()) {
              dirsToDelete.add(child.asDirectory());
            } else {
              mInodeStore.removeInodeAndParentEdge(inode);
              mInodeCounter.decrement();
            }
          }
        }
      }
    } else {
      mInodeStore.removeInodeAndParentEdge(inode);
      mInodeCounter.decrement();
    }
    if (inode.isFile()) {
      mBucketCounter.remove(inode.asFile().getLength());
    }

    String parentName = path.getParent().getFileName().toString();
    long grandpaId = getIdFromPath(path.getParent().getParent());

    updateTimestampsAndChildCount(grandpaId, parentName, entry.getOpTimeMs(), -1);
    mPinnedInodeFileIds.remove(id);
    mReplicationLimitedFileIds.remove(id);
    mToBePersistedIds.remove(id);
    mTtlBuckets.remove(inode);
  }

  private void createDirectory(InodeDirectoryEntry entry) {
    createInode(MutableInodeDirectory.fromJournalEntry(entry), entry.getPath());
  }

  private void createFile(InodeFileEntry entry) {
    createInode(MutableInodeFile.fromJournalEntry(entry), entry.getPath());
  }

  public void setAcl(long parentId, String name,
      SetAclAction action, List<AclEntry> values) {
    MutableInode<?> inode = mInodeStore.getMutable(parentId, name).get();
    switch (action) {
      case REPLACE:
        // fully replace the acl for the path
        inode.replaceAcl(values);
        break;
      case MODIFY:
        inode.setAcl(values);
        break;
      case REMOVE:
        inode.removeAcl(values);
        break;
      case REMOVE_ALL:
        inode.removeExtendedAcl();
        break;
      case REMOVE_DEFAULT:
        inode.setDefaultACL(new DefaultAccessControlList(inode.getACL()));
        break;
      default:
        LOG.warn("Unrecognized acl action: " + action);
    }
    mInodeStore.writeInode(inode);
  }

  private void applyUpdateInode(UpdateInodeEntry entry) {
    Optional<MutableInode<?>> inodeOpt = mInodeStore
        .getMutable(entry.getParentId(), entry.getName());
    if (!inodeOpt.isPresent()) {
      if (isJournalUpdateAsync(entry)) {
        // do not throw if the entry is journaled asynchronously
        return;
      }
      throw new IllegalStateException("Inode " + entry.getId() + " not found");
    }
    MutableInode<?> inode = inodeOpt.get();
    if (entry.hasTtl()) {
      // Remove before updating the inode. #remove relies on the inode having the same
      // TTL as when it was inserted.
      mTtlBuckets.remove(inode);
    }
    inode.updateFromEntry(entry);
    if (entry.hasTtl()) {
      mTtlBuckets.insert(Inode.wrap(inode));
    }
    if (inode.isFile() && entry.hasPinned()) {
      setReplicationForPin(inode, entry.getPinned());
    }
    mInodeStore.writeInode(inode);
    updateToBePersistedIds(inode);
  }

  private void setReplicationForPin(MutableInode<?> inode, boolean pinned) {
    if (inode.isFile()) {
      MutableInodeFile file = inode.asFile();
      if (pinned) {
        // when we pin a file with default min replication (zero), we bump the min replication
        // to one in addition to setting pinned flag, and adjust the max replication if it is
        // smaller than min replication.
        file.setPinned(true);
        if (file.getReplicationMin() == 0) {
          file.setReplicationMin(1);
        }
        if (file.getReplicationMax() == 0) {
          file.setReplicationMax(alluxio.Constants.REPLICATION_MAX_INFINITY);
        }
        mPinnedInodeFileIds.add(inode.getId());
      } else {
        // when we unpin a file, set the min replication to zero too.
        file.setReplicationMin(0);
        mPinnedInodeFileIds.remove(file.getId());
      }
      if (file.getReplicationMax() != alluxio.Constants.REPLICATION_MAX_INFINITY) {
        mReplicationLimitedFileIds.add(file.getId());
      }
    }
  }

  private OperationId getOpId(Supplier<JournalContext> context) {
    if (context instanceof RpcContext) {
      return ((RpcContext) context).getOpId();
    }
    return null;
  }

  /**
   * @param entry the update inode journal entry to be checked
   * @return whether the journal entry might be applied asynchronously out of order
   */
  private boolean isJournalUpdateAsync(UpdateInodeEntry entry) {
    return entry.getAllFields().size() == 2 && entry.hasId() && entry.hasLastAccessTimeMs();
  }

  public void setDirectChildrenLoaded(UpdateInodeDirectoryEntry entry) {
    MutableInode<?> inode = mInodeStore
        .getMutable(entry.getParentId(), entry.getName()).get();
    Preconditions.checkState(inode.isDirectory(),
        "Encountered non-directory id in update directory entry %s", entry);

    inode.asDirectory().updateFromEntry(entry);
    mInodeStore.writeInode(inode);
  }

  private void applyUpdateInodeFile(UpdateInodeFileEntry entry) {
    Path path = Paths.get(entry.getPath());
    Path parentPath = path.getParent();
    String name = path.getFileName().toString();
    long parentId = getIdFromPath(parentPath);
    MutableInode<?> inode = mInodeStore
        .getMutable(parentId, name).get();
    Preconditions.checkState(inode.isFile(), "Encountered non-file id in update file entry %s",
        entry);
    if (entry.hasReplicationMax()) {
      if (entry.getReplicationMax() == alluxio.Constants.REPLICATION_MAX_INFINITY) {
        mReplicationLimitedFileIds.remove(inode.getId());
      } else {
        mReplicationLimitedFileIds.add(inode.getId());
      }
    }
    if (inode.asFile().isCompleted()) {
      mBucketCounter.remove(inode.asFile().getLength());
    }
    inode.asFile().updateFromEntry(entry);
    mInodeStore.writeInode(inode);
    mBucketCounter.insert(inode.asFile().getLength());
  }

  ////
  /// Deprecated Entries
  ////

  private void applyAsyncPersist(AsyncPersistRequestEntry entry) {
    applyUpdateInode(UpdateInodeEntry.newBuilder().setId(entry.getFileId())
        .setPersistenceState(PersistenceState.TO_BE_PERSISTED.name()).build());
  }

  private void applyCompleteFile(CompleteFileEntry entry) {
    applyUpdateInode(UpdateInodeEntry.newBuilder().setId(entry.getId())
        .setLastModificationTimeMs(entry.getOpTimeMs()).setOverwriteModificationTime(true)
        .setUfsFingerprint(entry.getUfsFingerprint()).build());
    applyUpdateInodeFile(UpdateInodeFileEntry.newBuilder().setId(entry.getId())
        .setLength(entry.getLength()).addAllSetBlocks(entry.getBlockIdsList()).build());
  }

  private void applyInodeLastModificationTime(InodeLastModificationTimeEntry entry) {
    // This entry is deprecated, use UpdateInode instead.
    applyUpdateInode(UpdateInodeEntry.newBuilder().setId(entry.getId())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs()).build());
  }

  private void applyPersistDirectory(PersistDirectoryEntry entry) {
    // This entry is deprecated, use UpdateInode instead.
    applyUpdateInode(UpdateInodeEntry.newBuilder().setId(entry.getId())
        .setPersistenceState(PersistenceState.PERSISTED.name()).build());
  }

  private void setAttribute(SetAttributeEntry entry) {
    Builder builder = UpdateInodeEntry.newBuilder();
    builder.setId(entry.getId());
    if (entry.hasGroup()) {
      builder.setGroup(entry.getGroup());
    }
    if (entry.hasOpTimeMs()) {
      builder.setLastModificationTimeMs(entry.getOpTimeMs());
    }
    if (entry.hasOwner()) {
      builder.setOwner(entry.getOwner());
    }
    if (entry.hasPermission()) {
      builder.setMode((short) entry.getPermission());
    }
    if (entry.hasPersisted()) {
      if (entry.getPersisted()) {
        builder.setPersistenceState(PersistenceState.PERSISTED.name());
      } else {
        builder.setPersistenceState(PersistenceState.NOT_PERSISTED.name());
      }
    }
    if (entry.hasPinned()) {
      builder.setPinned(entry.getPinned());
    }
    if (entry.hasTtl()) {
      builder.setTtl(entry.getTtl());
      builder.setTtlAction(entry.getTtlAction());
    }
    if (entry.hasUfsFingerprint()) {
      builder.setUfsFingerprint(entry.getUfsFingerprint());
    }
    applyUpdateInode(builder.build());
  }

  ////
  // Helper methods
  ////

  private void createInode(MutableInode<?> inode, String path) {
    if (inode.isDirectory() && inode.getName().equals(InodeTree.ROOT_INODE_NAME)) {
      // This is the root inode. Clear all the state, and set the root.
      mInodeStore.clear();
      mInodeStore.writeNewInode(inode);
      mInodeCounter.reset();
      mInodeCounter.increment();
      mPinnedInodeFileIds.clear();
      mReplicationLimitedFileIds.clear();
      mToBePersistedIds.clear();

      updateToBePersistedIds(inode);
      return;
    }
    // inode should be added to the inode store before getting added to its parent list, because it
    // becomes visible at this point.
    mInodeStore.writeNewInode(inode);
    mInodeCounter.increment();
    mInodeStore.addChild(inode.getParentId(), inode);
    Path parentPath = Paths.get(path).getParent();
    String parentName = InodeTree.ROOT_INODE_NAME;
    long grandpaId = 0;
    if (!parentPath.toString().equals(InodeTree.ROOT_PATH)) {
      parentName = parentPath.getFileName().toString();
      grandpaId = getIdFromPath(parentPath.getParent());
    }
    // Only update size, last modified time is updated separately.
    updateTimestampsAndChildCount(grandpaId, parentName, Long.MIN_VALUE, 1);
    if (inode.isFile()) {
      boolean pinned = inode.asFile().isPinned() || inode.asFile().getReplicationMin() > 0;
      setReplicationForPin(inode, pinned);
    }
    // Add the file to TTL buckets, the insert automatically rejects files w/ Constants.NO_TTL
    mTtlBuckets.insert(Inode.wrap(inode));
    updateToBePersistedIds(inode);
    if (inode.isFile() && inode.asFile().isCompleted()) {
      mBucketCounter.insert(inode.asFile().getLength());
    }
  }

  private void applyRename(RenameEntry entry) {
    if (entry.hasDstPath()) {
      entry = rewriteDeprecatedRenameEntry(entry);
    }
    Path srcPath = Paths.get(entry.getPath());
    Path fileName = srcPath.getFileName();
    long oldParentId = getIdFromPath(srcPath.getParent());
    MutableInode<?> inode = mInodeStore
        .getMutable(oldParentId, fileName.toString()).get();
    long oldParent = inode.getParentId();
    long newParent = entry.getNewParentId();

    String oldParentName = "";
    long oldGrandpaId = 0;
    if (oldParent != 0) {
      // Old parent is not root
      oldParentName = srcPath.getParent().getFileName().toString();
      oldGrandpaId = getIdFromPath(srcPath.getParent().getParent());
    }

    Path path = Paths.get(entry.getDstPath());
    long newGrandpaId = 0;
    String newParentName = "";
    if (newParent != 0) {
      newGrandpaId = getIdFromPath(path.getParent().getParent());
      newParentName = path.getParent().getFileName().toString();
    }

    // TODO(yyong) to keep atomic, the journal required here.
    mInodeStore.removeChild(oldParent, inode.getName());
    inode.setName(entry.getNewName());
    inode.setParentId(newParent);
    mInodeStore.writeInode(inode);

    if (oldParent == newParent) {
      updateTimestampsAndChildCount(oldGrandpaId, oldParentName, entry.getOpTimeMs(), 0);
    } else {
      updateTimestampsAndChildCount(oldGrandpaId, oldParentName, entry.getOpTimeMs(), -1);
      updateTimestampsAndChildCount(newGrandpaId, newParentName, entry.getOpTimeMs(), 1);
    }
  }

  private void updateTimestampsAndChildCount(long parentId, String name,
      long opTimeMs, long deltaChildCount) {
    MutableInodeDirectory inode = mInodeStore
        .getMutable(parentId, name).get().asDirectory();
    try (LockResource lr = mInodeLockManager.lockUpdate(inode.getId())) {
      boolean madeUpdate = false;
      if (inode.getLastModificationTimeMs() < opTimeMs) {
        inode.setLastModificationTimeMs(opTimeMs);
        madeUpdate = true;
      }
      if (inode.getLastAccessTimeMs() < opTimeMs) {
        inode.setLastAccessTimeMs(opTimeMs);
        madeUpdate = true;
      }
      if (deltaChildCount != 0) {
        inode.setChildCount(inode.getChildCount() + deltaChildCount);
        madeUpdate = true;
      }
      if (madeUpdate) {
        // TODO(yyong) need to split the inode if the cache related attribute is changed
        mInodeStore.writeInode(inode);
      }
    }
  }

  private void updateToBePersistedIds(MutableInode<?> inode) {
    if (inode.getPersistenceState() == PersistenceState.TO_BE_PERSISTED) {
      mToBePersistedIds.add(inode.getId());
    } else {
      mToBePersistedIds.remove(inode.getId());
    }
  }

  private RenameEntry rewriteDeprecatedRenameEntry(RenameEntry entry) {
    Preconditions.checkState(!entry.hasNewName(),
        "old-style rename entries should not have the newName field set");
    Preconditions.checkState(!entry.hasNewParentId(),
        "old-style rename entries should not have the newParentId field set");
    Path path = Paths.get(entry.getDstPath());
    Path parent = path.getParent();
    Path filename = path.getFileName();
    if (parent == null) {
      throw new NullPointerException("path parent cannot be null");
    }
    if (filename == null) {
      throw new NullPointerException("path filename cannot be null");
    }

    return RenameEntry.newBuilder().setId(entry.getId())
        .setNewParentId(getIdFromPath(parent))
        .setNewName(filename.toString())
        .setOpTimeMs(entry.getOpTimeMs()).build();
  }

  public long getIdFromPath(Path path) {
    Inode curr = getRoot();
    for (Path component : path) {
      curr = mInodeStore.getChild(curr.asDirectory(), component.toString()).get();
    }
    return curr.getId();
  }

  public boolean processJournalEntry(JournalEntry entry) {
    // Apply entry.
    if (entry.hasDeleteFile()) {
      delete(entry.getDeleteFile());
    } else if (entry.hasInodeDirectory()) {
      createDirectory(entry.getInodeDirectory());
    } else if (entry.hasInodeFile()) {
      createFile(entry.getInodeFile());
    } else if (entry.hasRename()) {
      applyRename(entry.getRename());
    } else if (entry.hasUpdateInode()) {
      applyUpdateInode(entry.getUpdateInode());
    } else if (entry.hasUpdateInodeFile()) {
      applyUpdateInodeFile(entry.getUpdateInodeFile());
      // Deprecated entries
    } else if (entry.hasAsyncPersistRequest()) {
      applyAsyncPersist(entry.getAsyncPersistRequest());
    } else if (entry.hasCompleteFile()) {
      applyCompleteFile(entry.getCompleteFile());
    } else if (entry.hasInodeLastModificationTime()) {
      applyInodeLastModificationTime(entry.getInodeLastModificationTime());
    } else if (entry.hasPersistDirectory()) {
      applyPersistDirectory(entry.getPersistDirectory());
    } else if (entry.hasSetAttribute()) {
      setAttribute(entry.getSetAttribute());
    } else {
      return false;
    }

    // Account for entry in retry-cache before returning.
    if (entry.hasOperationId()) {
      cacheOperation(OperationId.fromJournalProto(entry.getOperationId()));
    }
    return true;
  }
}
