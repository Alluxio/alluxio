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

package alluxio.master.journalv0;

import alluxio.proto.journal.Journal.JournalEntry;

import java.io.IOException;

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
   * Marks all logs as completed.
   */
  void completeLogs() throws IOException;

  /**
   * Returns an output stream for the journal checkpoint. The returned output stream is a singleton
   * for this writer.
   *
   * @param latestSequenceNumber the sequence number of the latest journal entry. This sequence
   *        number will be used to determine the next sequence numbers for the subsequent journal
   *        entries.
   * @return the output stream for the journal checkpoint
   */
  JournalOutputStream getCheckpointOutputStream(long latestSequenceNumber) throws IOException;

  /**
   * Writes an entry to the current log stream. {@link #flush} should be called
   * afterward to ensure the entry is persisted.
   *
   * @param entry the journal entry to write
   */
  void write(JournalEntry entry) throws IOException;

  /**
   * Flushes the current log stream. Otherwise this operation is a no-op.
   */
  void flush() throws IOException;

  /**
   * @return the next sequence number
   */
  long getNextSequenceNumber();

  /**
   * Closes the journal.
   */
  void close() throws IOException;

  /**
   * Recovers the checkpoint in case the master crashed while updating it previously.
   */
  void recover();

  /**
   * Deletes all of the completed logs.
   */
  void deleteCompletedLogs() throws IOException;

  /**
   * Marks the current log as completed. If successful, the current log will no longer exist. The
   * current log must be closed before this call.
   */
  void completeCurrentLog() throws IOException;
}
