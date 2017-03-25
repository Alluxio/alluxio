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

import java.io.IOException;
import java.net.URI;

/**
 * This class manages all the writes to the journal. Journal writes happen in two phases:
 *
 * 1. First the checkpoint is written. The checkpoint contains entries reflecting the
 * state of the master with all of the completed logs applied.
 *
 * 2. Afterwards, entries are appended to log. The checkpoint must be written before the logs.
 *
 * The latest state can be reconstructed by reading the checkpoint, and applying all the
 * completed logs and finally the current log.
 */
public interface JournalWriter {
  /**
   * Writes an entry to the current log stream. {@link #flush} should be called
   * afterward to ensure the entry is persisted.
   *
   * @param entry the journal entry to write
   * @throws IOException if an error occurs writing the entry or if the checkpoint is not closed
   */
  void write(JournalEntry entry) throws IOException;

  /**
   * Flushes the current log stream. Otherwise this operation is a no-op.
   *
   * @throws IOException if an error occurs preventing the stream from being flushed
   */
  void flush() throws IOException;

  /**
   * Closes the journal.
   *
   * @throws IOException if an I/O error occurs
   */
  void close() throws IOException;
}
