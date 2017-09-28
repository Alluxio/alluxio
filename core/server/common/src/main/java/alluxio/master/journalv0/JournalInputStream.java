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
 * This input stream retrieves {@link JournalEntry} from journal checkpoints and journal logs.
 */
public interface JournalInputStream extends AutoCloseable {
  /**
   * Reads the next journal entry.
   *
   * @return the next {@link JournalEntry} in the stream, or null if the are no more entries
   */
  JournalEntry read() throws IOException;

  /**
   * Closes the stream.
   */
  void close() throws IOException;

  /**
   * @return the sequence number of the latest journal entry seen in the stream
   */
  long getLatestSequenceNumber();
}
