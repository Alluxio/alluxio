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

import alluxio.proto.journal.Journal.JournalEntry;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;

/**
 * Interface providing default implementations which do nothing.
 */
public interface NoopJournaled extends Journaled {
  @Override
  default boolean processJournalEntry(JournalEntry entry) {
    return false;
  }

  @Override
  default void resetState() {
  }

  @Override
  default CheckpointName getCheckpointName() {
    return CheckpointName.NOOP;
  }

  @Override
  default void writeToCheckpoint(OutputStream output) {
  }

  @Override
  default void restoreFromCheckpoint(InputStream input) {
  }

  @Override
  default Iterator<JournalEntry> getJournalEntryIterator() {
    return Collections.emptyIterator();
  }
}
