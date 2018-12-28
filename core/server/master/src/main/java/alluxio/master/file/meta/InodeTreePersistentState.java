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
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryReplayable;
import alluxio.master.metastore.InodeStore;
import alluxio.proto.journal.File.AsyncPersistRequestEntry;
import alluxio.proto.journal.File.CompleteFileEntry;
import alluxio.proto.journal.File.DeleteFileEntry;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.File.InodeLastModificationTimeEntry;
import alluxio.proto.journal.File.NewBlockEntry;
import alluxio.proto.journal.File.PersistDirectoryEntry;
import alluxio.proto.journal.File.ReinitializeFileEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAclEntry;
import alluxio.proto.journal.File.SetAttributeEntry;
import alluxio.proto.journal.File.UpdateInodeDirectoryEntry;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.File.UpdateInodeEntry.Builder;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.util.StreamUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Class for managing persistent inode tree state.
 *
 * This class owns all persistent inode tree state, and all inode tree modifications must go through
 * this class. To modify the inode tree, create a journal entry and call one of the applyAndJournal
 * methods.
 */
public class InodeTreePersistentState implements JournalEntryReplayable {
  private static final Logger LOG = LoggerFactory.getLogger(InodeTreePersistentState.class);

  private final InodeStore mInodeStore;

  /**
   * A set of inode ids representing pinned inode files. These are not part of the journaled state,
   * but this class keeps the set of pinned inodes up to date whenever the inode tree is modified.
   *
   * This class owns this set, and no other class can modify the set.
   */
  private final Set<Long> mPinnedInodeFileIds = new ConcurrentHashSet<>(64, 0.90f, 64);

  /** A set of inode ids whose replication max value is non-default. */
  private final Set<Long> mReplicationLimitedFileIds = new ConcurrentHashSet<>(64, 0.90f, 64);

  /**
   * @return an unmodifiable view of the replication limited file ids
   */
  public Set<Long> getReplicationLimitedFileIds() {
    return Collections.unmodifiableSet(mReplicationLimitedFileIds);
  }

  /**
   * TTL bucket list. The list is owned by InodeTree, and is only shared with
   * InodeTreePersistentState so that the list can be updated whenever inode tree state changes.
   */
  // TODO(andrew): Move ownership of the ttl bucket list to this class
  private final TtlBucketList mTtlBuckets;

  /**
   * @param inodeStore file store which holds inode metadata
   * @param ttlBucketList reference to the ttl bucket list so that the list can be updated when the
   *        inode tree is modified
   */
  public InodeTreePersistentState(InodeStore inodeStore, TtlBucketList ttlBucketList) {
    mInodeStore = inodeStore;
    mTtlBuckets = ttlBucketList;
  }

  /**
   * @return the root of the inode tree
   */
  public ReadOnlyInodeDirectory getRoot() {
    return (ReadOnlyInodeDirectory) mInodeStore.get(0).orElse(null);
  }

  /**
   * @return the pinned inode file ids;
   */
  public Set<Long> getPinnedInodeFileIds() {
    return Collections.unmodifiableSet(mPinnedInodeFileIds);
  }

  /**
   * Applies a journal entry to the inode tree state. This method should only be used during journal
   * replay. Otherwise, use one of the applyAndJournal methods.
   *
   * @param entry the entry
   * @return whether the journal entry was of a type recognized by the inode tree
   */
  public boolean replayJournalEntryFromJournal(JournalEntry entry) {
    if (entry.hasDeleteFile()) {
      apply(entry.getDeleteFile());
    } else if (entry.hasInodeDirectory()) {
      apply(entry.getInodeDirectory());
    } else if (entry.hasInodeFile()) {
      apply(entry.getInodeFile());
    } else if (entry.hasNewBlock()) {
      apply(entry.getNewBlock());
    } else if (entry.hasRename()) {
      apply(entry.getRename());
    } else if (entry.hasSetAcl()) {
      apply(entry.getSetAcl());
    } else if (entry.hasUpdateInode()) {
      apply(entry.getUpdateInode());
    } else if (entry.hasUpdateInodeDirectory()) {
      apply(entry.getUpdateInodeDirectory());
    } else if (entry.hasUpdateInodeFile()) {
      apply(entry.getUpdateInodeFile());
      // Deprecated entries
    } else if (entry.hasAsyncPersistRequest()) {
      apply(entry.getAsyncPersistRequest());
    } else if (entry.hasCompleteFile()) {
      apply(entry.getCompleteFile());
    } else if (entry.hasInodeLastModificationTime()) {
      apply(entry.getInodeLastModificationTime());
    } else if (entry.hasPersistDirectory()) {
      apply(entry.getPersistDirectory());
    } else if (entry.hasReinitializeFile()) {
      apply(entry.getReinitializeFile());
    } else if (entry.hasSetAttribute()) {
      apply(entry.getSetAttribute());
    } else {
      return false;
    }
    return true;
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
      apply(entry);
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
      long id = apply(entry);
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
   * @return whether the inode was successfully renamed. Returns false if another inode was
   *         concurrently added with the same name. On false return, no state is changed,
   *         and no journal entry is written
   */
  public boolean applyAndJournal(Supplier<JournalContext> context, RenameEntry entry) {
    try {
      if (applyRename(entry)) {
        context.get().append(JournalEntry.newBuilder().setRename(entry).build());
        return true;
      }
      return false;
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
      apply(entry);
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
      apply(entry);
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
      apply(entry);
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
      apply(entry);
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
  public void applyAndJournal(Supplier<JournalContext> context, Inode<?> inode) {
    try {
      applyInode(inode);
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

  private void apply(DeleteFileEntry entry) {
    long id = entry.getId();

    ReadOnlyInode inode = mInodeStore.get(id).get();

    mInodeStore.remove(inode);
    mInodeStore.removeChild(inode.getParentId(), inode.getName());
    updateLastModified(inode.getParentId(), entry.getOpTimeMs());
    mPinnedInodeFileIds.remove(id);
    mReplicationLimitedFileIds.remove(id);

    // The recursive option is only used by old versions.
    if (inode.isDirectory() && entry.getRecursive()) {
      Queue<ReadOnlyInodeDirectory> dirsToDelete = new ArrayDeque<>();
      dirsToDelete.add((ReadOnlyInodeDirectory) inode);
      while (!dirsToDelete.isEmpty()) {
        ReadOnlyInodeDirectory dir = dirsToDelete.poll();
        mInodeStore.remove(inode);
        for (ReadOnlyInode child : mInodeStore.getChildren(dir)) {
          if (child.isDirectory()) {
            dirsToDelete.add((ReadOnlyInodeDirectory) child);
          } else {
            mInodeStore.remove(inode);
          }
        }
      }
    }
  }

  private void apply(InodeDirectoryEntry entry) {
    applyInode(InodeDirectory.fromJournalEntry(entry));
  }

  private void apply(InodeFileEntry entry) {
    applyInode(InodeFile.fromJournalEntry(entry));
  }

  private long apply(NewBlockEntry entry) {
    InodeFile inode = (InodeFile) mInodeStore.getMutable(entry.getId()).get();
    long newBlockId = inode.getNewBlockId();
    mInodeStore.writeInode(inode);
    return newBlockId;
  }

  private void apply(RenameEntry entry) {
    if (entry.hasDstPath()) {
      entry = rewriteDeprecatedRenameEntry(entry);
    }
    Preconditions.checkState(applyRename(entry));
  }

  private void apply(SetAclEntry entry) {
    Inode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    List<AclEntry> entries = StreamUtils.map(AclEntry::fromProto, entry.getEntriesList());
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

  private void apply(UpdateInodeEntry entry) {
    Inode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    if (entry.hasTtl()) {
      // Remove before updating the inode. #remove relies on the inode having the same
      // TTL as when it was inserted.
      mTtlBuckets.remove(inode);
    }
    inode.updateFromEntry(entry);
    if (entry.hasTtl()) {
      mTtlBuckets.insert(ReadOnlyInode.wrap(inode));
    }
    if (entry.hasPinned() && inode.isFile()) {
      if (entry.getPinned()) {
        InodeFile file = (InodeFile) inode;
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
        ((InodeFile) inode).setReplicationMin(0);
        mPinnedInodeFileIds.remove(entry.getId());
      }
    }
    mInodeStore.writeInode(inode);
  }

  private void apply(UpdateInodeDirectoryEntry entry) {
    Inode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    Preconditions.checkState(inode.isDirectory(),
        "Encountered non-directory id in update directory entry %s", entry);
    InodeDirectory dir = (InodeDirectory) inode;

    dir.updateFromEntry(entry);
    mInodeStore.writeInode(inode);
  }

  private void apply(UpdateInodeFileEntry entry) {
    Inode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    Preconditions.checkState(inode.isFile(),
        "Encountered non-file id in update file entry %s", entry);
    if (entry.hasReplicationMax()) {
      if (entry.getReplicationMax() == alluxio.Constants.REPLICATION_MAX_INFINITY) {
        mReplicationLimitedFileIds.remove(inode.getId());
      } else {
        mReplicationLimitedFileIds.add(inode.getId());
      }
    }
    InodeFile file = (InodeFile) inode;

    file.updateFromEntry(entry);
    mInodeStore.writeInode(inode);
  }

  ////
  /// Deprecated Entries
  ////

  private void apply(AsyncPersistRequestEntry entry) {
    apply(UpdateInodeEntry.newBuilder()
        .setId(entry.getFileId())
        .setPersistenceState(PersistenceState.TO_BE_PERSISTED.name())
        .build());
  }

  private void apply(CompleteFileEntry entry) {
    apply(UpdateInodeEntry.newBuilder()
        .setId(entry.getId())
        .setLastModificationTimeMs(entry.getOpTimeMs())
        .setOverwriteModificationTime(true)
        .setUfsFingerprint(entry.getUfsFingerprint())
        .build());
    apply(UpdateInodeFileEntry.newBuilder()
        .setId(entry.getId())
        .setLength(entry.getLength())
        .addAllSetBlocks(entry.getBlockIdsList())
        .build());
  }

  private void apply(InodeLastModificationTimeEntry entry) {
    // This entry is deprecated, use UpdateInode instead.
    apply(UpdateInodeEntry.newBuilder()
        .setId(entry.getId())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs())
        .build());
  }

  private void apply(PersistDirectoryEntry entry) {
    // This entry is deprecated, use UpdateInode instead.
    apply(UpdateInodeEntry.newBuilder()
        .setId(entry.getId())
        .setPersistenceState(PersistenceState.PERSISTED.name())
        .build());
  }

  private void apply(ReinitializeFileEntry entry) {
    throw new UnsupportedOperationException("Lineage is not currently supported");
  }

  private void apply(SetAttributeEntry entry) {
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
    apply(builder.build());
  }

  ////
  // Helper methods
  ////

  private void applyInode(Inode<?> inode) {
    if (inode.isDirectory() && inode.getName().equals(InodeTree.ROOT_INODE_NAME)) {
      // This is the root inode. Clear all the state, and set the root.
      mInodeStore.clear();
      mInodeStore.writeInode(inode);
      mPinnedInodeFileIds.clear();
      return;
    }
    // inode should be added to the inode store before getting added to its parent list, because it
    // becomes visible at this point.
    mInodeStore.writeInode(inode);
    mInodeStore.addChild(inode.getParentId(), inode);
    if (inode.isFile()) {
      InodeFile file = (InodeFile) inode;
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
    mTtlBuckets.insert(ReadOnlyInode.wrap(inode));
  }

  private boolean applyRename(RenameEntry entry) {
    Inode<?> inode = mInodeStore.getMutable(entry.getId()).get();
    long oldParent = inode.getParentId();
    long newParent = entry.getNewParentId();
    mInodeStore.removeChild(oldParent, inode.getName());

    inode.setName(entry.getNewName());
    mInodeStore.addChild(newParent, inode);
    inode.setParentId(newParent);
    mInodeStore.writeInode(inode);
    updateLastModified(oldParent, entry.getOpTimeMs());
    updateLastModified(newParent, entry.getOpTimeMs());
    return true;
  }

  private void updateLastModified(long id, long opTimeMs) {
    Inode<?> inode = mInodeStore.getMutable(id).get();
    inode.setLastModificationTimeMs(opTimeMs);
    mInodeStore.writeInode(inode);
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
    ReadOnlyInode curr = getRoot();
    for (Path component : path) {
      curr = mInodeStore.getChild(curr.asDirectory(), component.toString()).get();
    }
    return curr.getId();
  }

  /**
   * Resets the inode tree state.
   */
  public void reset() {
    mInodeStore.clear();
    mReplicationLimitedFileIds.clear();
    mPinnedInodeFileIds.clear();
  }
}
