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
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;

/**
 * Interface providing default implementations which do nothing.
 */
public interface NoopJournaled extends Journaled {
  @Override
  default boolean processJournalEntry(JournalEntry entry) {
    return true;
  }

  @Override
  default void resetState() {
  }

  @Override
  default CheckpointName getCheckpointName() {
    return CheckpointName.NOOP;
  }

  @Override
  default void writeToCheckpoint(OutputStream output) throws IOException {
    // Just write a checkpoint type with no data. The stream constructor writes unbuffered to the
    // underlying output, so we don't need to flush or close.
    new CheckpointOutputStream(output, CheckpointType.JOURNAL_ENTRY);
  }

  @Override
  default void restoreFromCheckpoint(CheckpointInputStream input) {
  }

  @Override
  default CloseableIterator<JournalEntry> getJournalEntryIterator() {
    return CloseableIterator.noopCloseable(Collections.emptyIterator());
  }
}
