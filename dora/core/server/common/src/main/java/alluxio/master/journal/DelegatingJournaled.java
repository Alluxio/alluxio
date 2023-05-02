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
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

/**
 * Convenient interface for classes which delegate their journaling methods to another journaled
 * object.
 */
public interface DelegatingJournaled extends Journaled {
  @Override
  default boolean processJournalEntry(JournalEntry entry) {
    return getDelegate().processJournalEntry(entry);
  }

  @Override
  default void resetState() {
    getDelegate().resetState();
  }

  @Override
  default void applyAndJournal(Supplier<JournalContext> context, JournalEntry entry) {
    getDelegate().applyAndJournal(context, entry);
  }

  @Override
  default CheckpointName getCheckpointName() {
    return getDelegate().getCheckpointName();
  }

  @Override
  default void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    getDelegate().writeToCheckpoint(output);
  }

  @Override
  default void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    getDelegate().restoreFromCheckpoint(input);
  }

  @Override
  default CloseableIterator<JournalEntry> getJournalEntryIterator() {
    return getDelegate().getJournalEntryIterator();
  }

  /**
   * @return the object to delegate to
   */
  Journaled getDelegate();
}
