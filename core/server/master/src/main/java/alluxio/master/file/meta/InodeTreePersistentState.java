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

import alluxio.ProcessUtils;
import alluxio.collections.ConcurrentHashSet;
import alluxio.master.journal.CheckpointName;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.metastore.InodeStore;
import alluxio.proto.journal.File.AsyncPersistRequestEntry;
import alluxio.proto.journal.File.CompleteFileEntry;
import alluxio.proto.journal.File.DeleteFileEntry;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.File.InodeLastModificationTimeEntry;
import alluxio.proto.journal.File.NewBlockEntry;
import alluxio.proto.journal.File.PersistDirectoryEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAclEntry;
import alluxio.proto.journal.File.SetAttributeEntry;
import alluxio.proto.journal.File.UpdateInodeDirectoryEntry;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.File.UpdateInodeEntry.Builder;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.util.StreamUtils;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Class for managing persistent inode tree state.
 *
 * This class owns all persistent inode tree state, and all inode tree modifications must go through
 * this class. To modify the inode tree, create a journal entry and call one of the applyAndJournal
 * methods.
 */
public class InodeTreePersistentState implements Journaled {
  private static final Logger LOG = LoggerFactory.getLogger(InodeTreePersistentState.class);

  private final InodeStore mInodeStore;
  private final InodeLockManager mInodeLockManager;

  /**
   * A set of inode ids representing pinned inode files. These are not part of the journaled state,
   * but this class keeps the set of pinned inodes up to date whenever the inode tree is modified.
   *
   * This class owns this set, and no other class can modify the set.
   */
  private final Set<Long> mPinnedInodeFileIds = new ConcurrentHashSet<>(64, 0.90f, 64);

  /** A set of inode ids whose replication max value is non-default. */
  private final Set<Long> mReplicationLimitedFileIds = new ConcurrentHashSet<>(64, 0.90f, 64);

  /** A set of inode ids whose persistence state is {@link PersistenceState#TO_BE_PERSISTED}. */
  private final Set<Long> mToBePersistedIds = new ConcurrentHashSet<>(64, 0.90f, 64);

  /** Counter for tracking how many inodes we have. */
  private final AtomicLong mInodeCounter = new AtomicLong();

  /**
   * @return an unmodifiable view of the files with persistence state
   *         {@link PersistenceState#TO_BE_PERSISTED}
   */
  public Set<Long> getToBePersistedIds() {
    return Collections.unmodifiableSet(mToBePersistedIds);
  }

  /**
   * TTL bucket list. The list is owned by InodeTree, and is only shared with
   * InodeTreePersistentState so that the list can be updated whenever inode tree state changes.
   */
  // TODO(andrew): Move ownership of the ttl bucket list to this class
  private final TtlBucketList mTtlBuckets;

  /**
   * @param inodeStore file store which holds inode metadata
   * @param lockManager manager for inode locks
   * @param ttlBucketList reference to the ttl bucket list so that the list can be updated when the
   *        inode tree is modified
   */
  public InodeTreePersistentState(InodeStore inodeStore, InodeLockManager lockManager,
      TtlBucketList ttlBucketList) {
    mInodeStore = inodeStore;
    mInodeLockManager = lockManager;
    mTtlBuckets = ttlBucketList;
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
    return mInodeStore.get(0).map(Inode::asDirectory).orElse(null);
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
    return mInodeCounter.get();
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
    try {
      context.get().append(JournalEntry.newBuilder().setDeleteFile(entry).build());
      applyDelete(entry);
    } catch (Throwable t) {
      // Delete entries should always apply cleanly, but if it somehow fails, we are in a state
      // where we've journaled the delete, but failed to make the in-memory update. We don't yet
      // have a way to recover from this, so we give a fatal error.
      ProcessUtils.fatalError(LOG, t, "Failed to apply entry %s", entry);
    }
  }

  /**
   * Allocates and returns the next block ID for the indicated inode.
   *
   * @param context journal context supplier
   * @param entry new block entry
   * @return the new block id
   */
  public long applyAndJournal(Supplier<JournalContext> context, NewBlockEntry entry) {
    try {
      long id = applyNewBlock(entry);
      context.get().append(JournalEntry.newBuilder().setNewBlock(entry).build());
      return id;
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  /**
   * Renames an inode.
   *
   * @param context journal context supplier
   * @param entry rename entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, RenameEntry entry) {
    try {
      applyRename(entry);
      context.get().append(JournalEntry.newBuilder().setRename(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  /**
   * Sets an ACL for an inode.
   *
   * @param context journal context supplier
   * @param entry set acl entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, SetAclEntry entry) {
    try {
      applySetAcl(entry);
      context.get().append(JournalEntry.newBuilder().setSetAcl(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  /**
   * Updates an inode's state. This is used for state common to both files and directories.
   *
   * @param context journal context supplier
   * @param entry update inode entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, UpdateInodeEntry entry) {
    try {
      applyUpdateInode(entry);
      context.get().append(JournalEntry.newBuilder().setUpdateInode(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  /**
   * Updates an inode directory's state.
   *
   * @param context journal context supplier
   * @param entry update inode directory entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, UpdateInodeDirectoryEntry entry) {
    try {
      applyUpdateInodeDirectory(entry);
      context.get().append(JournalEntry.newBuilder().setUpdateInodeDirectory(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  /**
   * Updates an inode file's state.
   *
   * @param context journal context supplier
   * @param entry update inode file entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, UpdateInodeFileEntry entry) {
    try {
      applyUpdateInodeFile(entry);
      context.get().append(JournalEntry.newBuilder().setUpdateInodeFile(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  /**
   * Adds an inode to the inode tree.
   *
   * @param context journal context supplier
   * @param inode an inode to add and create a journal entry for
   */
  public void applyAndJournal(Supplier<JournalContext> context, MutableInode<?> inode) {
    try {
      applyCreateInode(inode);
      context.get().append(inode.toJournalEntry());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", inode);
      throw t; // fatalError will usually system.exit
    }
  }

  ////
  /// Apply Implementations. These methods are used for journal replay, so they are not allowed to
  /// fail. They are also used when making metadata changes during regular operation.
  ////

  private void applyDelete(DeleteFileEntry entry) {
    long id = entry.getId();
    Inode inode = mInodeStore.get(id).get();

    // The recursive option is only used by old versions.
    if (inode.isDirectory() && entry.getRecursive()) {
      Queue<InodeDirectory> dirsToDelete = new ArrayDeque<>();
      dirsToDelete.add(inode.asDirectory());
      while (!dirsToDelete.isEmpty()) {
        InodeDirectory dir = dirsToDelete.poll();
        mInodeStore.removeInodeAndParentEdge(inode);
        mInodeCounter.decrementAndGet();
        for (Inode child : mInodeStore.getChildren(dir)) {
          if (child.isDirectory()) {
            dirsToDelete.add(child.asDirectory());
          } else {
            mInodeStore.removeInodeAndParentEdge(inode);
            mInodeCounter.decrementAndGet();
          }
        }
      }
    } else {
      mInodeStore.removeInodeAndParentEdge(inode);
      mInodeCounter.decrementAndGet();
    }

    updateLastModifiedAndChildCount(inode.getParentId(), entry.getOpTimeMs(), -1);
    mPinnedInodeFileIds.remove(id);
    mReplicationLimitedFileIds.remove(id);
    mToBePersistedIds.remove(id);
  }

  private void applyCreateDirectory(InodeDirectoryEntry entry) {
    applyCreateInode(MutableInodeDirectory.fromJournalEntry(entry));
  }

  private void applyCreateFile(InodeFileEntry entry) {
    applyCreateInode(MutableInodeFile.fromJournalEntry(entry));
  }

  private long applyNewBlock(NewBlockEntry entry) {
    MutableInodeFile inode = mInodeStore.getMutable(entry.getId()).get().asFile();
    long newBlockId = inode.getNewBlockId();
    mInodeStore.writeInode(inode);
    return newBlockId;
  }

  private void applySetAcl(SetAclEntry entry) {
    MutableInode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    List<AclEntry> entries = StreamUtils.map(ProtoUtils::fromProto, entry.getEntriesList());
    switch (entry.getAction()) {
      case REPLACE:
        // fully replace the acl for the path
        inode.replaceAcl(entries);
        break;
      case MODIFY:
        inode.setAcl(entries);
        break;
      case REMOVE:
        inode.removeAcl(entries);
        break;
      case REMOVE_ALL:
        inode.removeExtendedAcl();
        break;
      case REMOVE_DEFAULT:
        inode.setDefaultACL(new DefaultAccessControlList(inode.getACL()));
        break;
      default:
        LOG.warn("Unrecognized acl action: " + entry.getAction());
    }
    mInodeStore.writeInode(inode);
  }

  private void applyUpdateInode(UpdateInodeEntry entry) {
    MutableInode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    if (entry.hasTtl()) {
      // Remove before updating the inode. #remove relies on the inode having the same
      // TTL as when it was inserted.
      mTtlBuckets.remove(inode);
    }
    inode.updateFromEntry(entry);
    if (entry.hasTtl()) {
      mTtlBuckets.insert(Inode.wrap(inode));
    }
    if (entry.hasPinned() && inode.isFile()) {
      if (entry.getPinned()) {
        MutableInodeFile file = inode.asFile();
        // when we pin a file with default min replication (zero), we bump the min replication
        // to one in addition to setting pinned flag, and adjust the max replication if it is
        // smaller than min replication.
        if (file.getReplicationMin() == 0) {
          file.setReplicationMin(1);
          if (file.getReplicationMax() == 0) {
            file.setReplicationMax(alluxio.Constants.REPLICATION_MAX_INFINITY);
          }
        }
        mPinnedInodeFileIds.add(entry.getId());
      } else {
        // when we unpin a file, set the min replication to zero too.
        inode.asFile().setReplicationMin(0);
        mPinnedInodeFileIds.remove(entry.getId());
      }
    }
    mInodeStore.writeInode(inode);
    updateToBePersistedIds(inode);
  }

  private void applyUpdateInodeDirectory(UpdateInodeDirectoryEntry entry) {
    MutableInode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    Preconditions.checkState(inode.isDirectory(),
        "Encountered non-directory id in update directory entry %s", entry);

    inode.asDirectory().updateFromEntry(entry);
    mInodeStore.writeInode(inode);
  }

  private void applyUpdateInodeFile(UpdateInodeFileEntry entry) {
    MutableInode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    Preconditions.checkState(inode.isFile(),
        "Encountered non-file id in update file entry %s", entry);
    if (entry.hasReplicationMax()) {
      if (entry.getReplicationMax() == alluxio.Constants.REPLICATION_MAX_INFINITY) {
        mReplicationLimitedFileIds.remove(inode.getId());
      } else {
        mReplicationLimitedFileIds.add(inode.getId());
      }
    }
    inode.asFile().updateFromEntry(entry);
    mInodeStore.writeInode(inode);
  }

  ////
  /// Deprecated Entries
  ////

  private void applyAsyncPersist(AsyncPersistRequestEntry entry) {
    applyUpdateInode(UpdateInodeEntry.newBuilder()
        .setId(entry.getFileId())
        .setPersistenceState(PersistenceState.TO_BE_PERSISTED.name())
        .build());
  }

  private void applyCompleteFile(CompleteFileEntry entry) {
    applyUpdateInode(UpdateInodeEntry.newBuilder()
        .setId(entry.getId())
        .setLastModificationTimeMs(entry.getOpTimeMs())
        .setOverwriteModificationTime(true)
        .setUfsFingerprint(entry.getUfsFingerprint())
        .build());
    applyUpdateInodeFile(UpdateInodeFileEntry.newBuilder()
        .setId(entry.getId())
        .setLength(entry.getLength())
        .addAllSetBlocks(entry.getBlockIdsList())
        .build());
  }

  private void applyInodeLastModificationTime(InodeLastModificationTimeEntry entry) {
    // This entry is deprecated, use UpdateInode instead.
    applyUpdateInode(UpdateInodeEntry.newBuilder()
        .setId(entry.getId())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs())
        .build());
  }

  private void applyPersistDirectory(PersistDirectoryEntry entry) {
    // This entry is deprecated, use UpdateInode instead.
    applyUpdateInode(UpdateInodeEntry.newBuilder()
        .setId(entry.getId())
        .setPersistenceState(PersistenceState.PERSISTED.name())
        .build());
  }

  private void applySetAttribute(SetAttributeEntry entry) {
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

  private void applyCreateInode(MutableInode<?> inode) {
    if (inode.isDirectory() && inode.getName().equals(InodeTree.ROOT_INODE_NAME)) {
      // This is the root inode. Clear all the state, and set the root.
      mInodeStore.clear();
      mInodeStore.writeNewInode(inode);
      mInodeCounter.set(1);
      mPinnedInodeFileIds.clear();
      mReplicationLimitedFileIds.clear();
      mToBePersistedIds.clear();

      updateToBePersistedIds(inode);
      return;
    }
    // inode should be added to the inode store before getting added to its parent list, because it
    // becomes visible at this point.
    mInodeStore.writeNewInode(inode);
    mInodeCounter.incrementAndGet();
    mInodeStore.addChild(inode.getParentId(), inode);
    // Only update size, last modified time is updated separately.
    updateLastModifiedAndChildCount(inode.getParentId(), Long.MIN_VALUE, 1);
    if (inode.isFile()) {
      MutableInodeFile file = inode.asFile();
      if (file.getReplicationMin() > 0) {
        mPinnedInodeFileIds.add(file.getId());
        file.setPinned(true);
      }
      if (file.getReplicationMax() != alluxio.Constants.REPLICATION_MAX_INFINITY) {
        mReplicationLimitedFileIds.add(file.getId());
      }
    }
    // Update indexes.
    if (inode.isFile() && inode.isPinned()) {
      mPinnedInodeFileIds.add(inode.getId());
    }
    // Add the file to TTL buckets, the insert automatically rejects files w/ Constants.NO_TTL
    mTtlBuckets.insert(Inode.wrap(inode));
    updateToBePersistedIds(inode);
  }

  private void applyRename(RenameEntry entry) {
    if (entry.hasDstPath()) {
      entry = rewriteDeprecatedRenameEntry(entry);
    }

    MutableInode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    long oldParent = inode.getParentId();
    long newParent = entry.getNewParentId();

    mInodeStore.removeChild(oldParent, inode.getName());
    inode.setName(entry.getNewName());
    mInodeStore.addChild(newParent, inode);
    inode.setParentId(newParent);
    mInodeStore.writeInode(inode);

    if (oldParent == newParent) {
      updateLastModifiedAndChildCount(oldParent, entry.getOpTimeMs(), 0);
    } else {
      updateLastModifiedAndChildCount(oldParent, entry.getOpTimeMs(), -1);
      updateLastModifiedAndChildCount(newParent, entry.getOpTimeMs(), 1);
    }
  }

  /**
   * Updates the last modified time (LMT) for the indicated inode directory, and updates its child
   * count.
   *
   * If the inode's LMT is already greater than the specified time, the inode's LMT will not be
   * changed.
   *
   * @param id the inode to update
   * @param opTimeMs the time of the operation that modified the inode
   * @param deltaChildCount the change in inode directory child count
   */
  private void updateLastModifiedAndChildCount(long id, long opTimeMs, long deltaChildCount) {
    try (LockResource lr = mInodeLockManager.lockUpdate(id)) {
      MutableInodeDirectory inode = mInodeStore.getMutable(id).get().asDirectory();
      boolean madeUpdate = false;
      if (inode.getLastModificationTimeMs() < opTimeMs) {
        inode.setLastModificationTimeMs(opTimeMs);
        madeUpdate = true;
      }
      if (deltaChildCount != 0) {
        inode.setChildCount(inode.getChildCount() + deltaChildCount);
        madeUpdate = true;
      }
      if (madeUpdate) {
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
    return RenameEntry.newBuilder()
        .setId(entry.getId())
        .setNewParentId(getIdFromPath(path.getParent()))
        .setNewName(path.getFileName().toString())
        .setOpTimeMs(entry.getOpTimeMs())
        .build();
  }

  private long getIdFromPath(Path path) {
    Inode curr = getRoot();
    for (Path component : path) {
      curr = mInodeStore.getChild(curr.asDirectory(), component.toString()).get();
    }
    return curr.getId();
  }

  @Override
  public boolean processJournalEntry(JournalEntry entry) {
    if (entry.hasDeleteFile()) {
      applyDelete(entry.getDeleteFile());
    } else if (entry.hasInodeDirectory()) {
      applyCreateDirectory(entry.getInodeDirectory());
    } else if (entry.hasInodeFile()) {
      applyCreateFile(entry.getInodeFile());
    } else if (entry.hasNewBlock()) {
      applyNewBlock(entry.getNewBlock());
    } else if (entry.hasRename()) {
      applyRename(entry.getRename());
    } else if (entry.hasSetAcl()) {
      applySetAcl(entry.getSetAcl());
    } else if (entry.hasUpdateInode()) {
      applyUpdateInode(entry.getUpdateInode());
    } else if (entry.hasUpdateInodeDirectory()) {
      applyUpdateInodeDirectory(entry.getUpdateInodeDirectory());
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
      applySetAttribute(entry.getSetAttribute());
    } else {
      return false;
    }
    return true;
  }

  @Override
  public void resetState() {
    mInodeStore.clear();
    mReplicationLimitedFileIds.clear();
    mPinnedInodeFileIds.clear();
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    // Write tree via breadth-first traversal, so that during deserialization, it may be more
    // efficient than depth-first during deserialization due to parent directory's locality.
    Queue<Inode> inodes = new LinkedList<>();
    if (getRoot() != null) {
      inodes.add(getRoot());
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
        Inode inode = inodes.poll();
        if (inode.isDirectory()) {
          Iterables.addAll(inodes, mInodeStore.getChildren(inode.asDirectory()));
        }
        return inode.toJournalEntry();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove is not supported in inode tree iterator");
      }
    };
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.INODE_TREE;
  }
}
