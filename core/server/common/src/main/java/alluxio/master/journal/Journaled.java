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

package alluxio.master.journal;

import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.Checkpointed;
import alluxio.proto.journal.Journal.JournalEntry;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

/**
 * Interface for classes with journaled state. This class provides a default checkpointing
 * implementation which writes checkpoints based on the journal entries from
 * {@link JournalEntryIterable#getJournalEntryIterator()}.
 */
public interface Journaled extends Checkpointed, JournalEntryIterable {
  /**
   * Attempts to apply a journal entry.
   *
   * @param entry the entry to apply
   * @return whether the entry type is supported by this journaled object
   */
  boolean processJournalEntry(JournalEntry entry);

  /**
   * Resets the object's journaled state.
   */
  void resetState();

  /**
   * Applies and journals a journal entry.
   *
   * All journal appends should go through this method.
   *
   * @param context journal context
   * @param entry the entry to apply and journal
   */
  default void applyAndJournal(Supplier<JournalContext> context, JournalEntry entry) {
    processJournalEntry(entry);
    context.get().append(entry);
  }

  @Override
  default void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    JournalUtils.writeJournalEntryCheckpoint(output, this);
  }

  @Override
  default void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    JournalUtils.restoreJournalEntryCheckpoint(input, this);
  }
}
