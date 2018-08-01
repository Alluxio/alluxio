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

package alluxio.master.file;

import alluxio.collections.FieldIndex;
import alluxio.collections.IndexDefinition;
import alluxio.collections.UniqueFieldIndex;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.PersistenceState;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;

/**
 * Coordinator for persisted master state, i.e. state that must be journaled, and which can be
 * restored from the journal in case of master failure.
 *
 * As we move more master functionality behind this abstraction, this class will grow to eventually
 * own all master state.
 */
public final class PersistentFsMasterState {
  private static final IndexDefinition<Inode<?>> ID_INDEX = new IndexDefinition<Inode<?>>(true) {
    @Override
    public Object getFieldValue(Inode<?> o) {
      return o.getId();
    }
  };

  /** Use UniqueFieldIndex directly for ID index rather than using IndexedSet. */
  private final FieldIndex<Inode<?>> mInodes = new UniqueFieldIndex<>(ID_INDEX);

  /**
   * @return the inodes of the inode tree, indexed by id
   */
  public FieldIndex<Inode<?>> getInodes() {
    // Once all state changes are captured within this class, we will change this to return an
    // immutable view.
    return mInodes;
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
    }
  }

  /**
   * Applies a journal entry and records it in the rpc context.
   *
   * @param rpcContext the rpc context
   * @param entry the entry
   */
  public void applyAndJournal(RpcContext rpcContext, JournalEntry entry) {
    apply(entry);
    rpcContext.journal(entry);
  }
}
