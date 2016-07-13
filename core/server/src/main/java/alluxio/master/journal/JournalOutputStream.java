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
 * This output stream writes {@link JournalEntry} objects to the journal. This output stream can
 * write to both the journal checkpoint file, or the journal log files.
 */
public interface JournalOutputStream {
  /**
   * Writes a {@link JournalEntry} to the journal.
   *
   * @param entry the entry to write to the journal
   * @throws IOException if an I/O error occurs
   */
  void writeEntry(JournalEntry entry) throws IOException;

  /**
   * Closes the stream.
   *
   * @throws IOException if an I/O error occurs
   */
  void close() throws IOException;

  /**
   * Flushes the stream.
   *
   * @throws IOException if an I/O error occurs
   */
  void flush() throws IOException;
}
