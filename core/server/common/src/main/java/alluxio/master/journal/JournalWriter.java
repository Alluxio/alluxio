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

/**
 * This class manages all the writes to the journal. Journal writes happen in two phases:
 *
 * 1. First the checkpoint file is written. The checkpoint file contains entries reflecting the
 * state of the master with all of the completed logs applied.
 *
 * 2. Afterwards, entries are appended to log files. The checkpoint file must be written before the
 * log files.
 *
 * The latest state can be reconstructed by reading the checkpoint file, and applying all the
 * completed logs and then the remaining log in progress.
 */
public interface JournalWriter {

  /**
   * Marks all logs as completed.
   *
   * @throws IOException if an I/O error occurs
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
   * @throws IOException if an I/O error occurs
   */
  JournalOutputStream getCheckpointOutputStream(long latestSequenceNumber) throws IOException;

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
   * @return the next sequence number
   */
  long getNextSequenceNumber();

  /**
   * Closes the journal.
   *
   * @throws IOException if an I/O error occurs
   */
  void close() throws IOException;

  /**
   * Recovers the checkpoint file in case the master crashed while updating it previously.
   */
  void recover();

  /**
   * Deletes all of the completed logs.
   *
   * @throws IOException if an I/O error occurs
   */
  void deleteCompletedLogs() throws IOException;

  /**
   * Moves the current log file to the completed folder, marking it as complete. If successful, the
   * current log file will no longer exist. The current log must already be closed before this call.
   *
   * @throws IOException if an I/O error occurs
   */
  void completeCurrentLog() throws IOException;
}
