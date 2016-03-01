/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
 * This input stream retrieves {@link JournalEntry} from journal checkpoint files and journal log
 * files.
 */
public interface JournalInputStream {
  /**
   * @return the next {@link JournalEntry} in the stream, null if the are no more entries in the
   *         stream
   * @throws IOException if a non-Alluxio related exception occurs
   */
  JournalEntry getNextEntry() throws IOException;

  /**
   * Closes the stream.
   *
   * @throws IOException if an I/O error occurs
   */
  void close() throws IOException;

  /**
   * @return the sequence number of the latest journal entry seen in the stream
   */
  long getLatestSequenceNumber();
}
