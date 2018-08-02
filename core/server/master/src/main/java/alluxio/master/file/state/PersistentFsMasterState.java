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

package alluxio.master.file.state;

import alluxio.collections.FieldIndex;
import alluxio.collections.IndexDefinition;
import alluxio.collections.UniqueFieldIndex;
import alluxio.master.file.RpcContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.state.DirectoryId.UnmodifiableDirectoryId;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.proto.journal.File.InodeDirectoryIdGeneratorEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import java.util.Iterator;

/**
 * Coordinator for persisted master state, i.e. state that must be journaled, and which can be
 * restored from the journal in case of master failure.
 *
 * As we move more master functionality behind this abstraction, this class will grow to eventually
 * own all master state.
 */
public final class PersistentFsMasterState implements JournalEntryIterable {
  private static final IndexDefinition<Inode<?>> ID_INDEX = new IndexDefinition<Inode<?>>(true) {
    @Override
    public Object getFieldValue(Inode<?> o) {
      return o.getId();
    }
  };

  /** Use UniqueFieldIndex directly for ID index rather than using IndexedSet. */
  private final FieldIndex<Inode<?>> mInodes = new UniqueFieldIndex<>(ID_INDEX);
  private final DirectoryId mNextDirectoryId = new DirectoryId();

  /**
   * @return the inodes of the inode tree, indexed by id
   */
  public FieldIndex<Inode<?>> getInodes() {
    // Once all state changes are captured within this class, we will change this to return an
    // immutable view.
    return mInodes;
  }

  /**
   * @return the next directory id
   */
  public UnmodifiableDirectoryId getNextDirectoryId() {
    return mNextDirectoryId.getImmutableView();
  }

  /**
   * Applies a journal entry to the master state. This method should only be used during journal
   * replay. Otherwise, use {@link #applyAndJournal(RpcContext, JournalEntry)}.
   *
   * @param entry the entry
   */
  public void apply(JournalEntry entry) {
    if (entry.hasPersistDirectory()) {
      Inode<?> dir = mInodes.getFirst(entry.getPersistDirectory().getId());
      Preconditions.checkState(dir.isDirectory(),
          "Encountered non-directory id in persist directory entry");
      dir.setPersistenceState(PersistenceState.PERSISTED);
    } else if (entry.hasInodeDirectoryIdGenerator()) {
      InodeDirectoryIdGeneratorEntry idEntry = entry.getInodeDirectoryIdGenerator();
      mNextDirectoryId.setContainerId(idEntry.getContainerId());
      mNextDirectoryId.setSequenceNumber(idEntry.getSequenceNumber());
    }
  }

  /**
   * Applies a journal entry and records it in the journal.
   *
   * @param rpcContext rpc context
   * @param entry the entry
   */
  public void applyAndJournal(RpcContext rpcContext, JournalEntry entry) {
    applyAndJournal(rpcContext.getJournalContext(), entry);
  }

  /**
   * Applies a journal entry and records it in the journal.
   *
   * @param journalContext journal context
   * @param entry the entry
   */
  public void applyAndJournal(JournalContext journalContext, JournalEntry entry) {
    apply(entry);
    journalContext.append(entry);
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return CommonUtils.singleElementIterator(InodeDirectoryIdGenerator
        .toEntry(mNextDirectoryId.getContainerId(), mNextDirectoryId.getSequenceNumber()));
  }
}
