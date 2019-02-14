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

import alluxio.exception.JournalClosedException;
import alluxio.proto.journal.Journal.JournalEntry;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for a class that can write and flush journal entries.
 */
public interface JournalWriter extends Closeable {
  /**
   * Writes an entry. {@link #flush} should be called afterwards if we want to make sure the entry
   * is persisted.
   *
   * @param entry the journal entry to write
   */
  void write(JournalEntry entry) throws IOException, JournalClosedException;

  /**
   * Flushes all the entries written to the underlying storage.
   */
  void flush() throws IOException, JournalClosedException;
}
