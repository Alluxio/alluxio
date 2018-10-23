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

import alluxio.exception.InvalidJournalEntryException;
import alluxio.proto.journal.Journal.JournalEntry;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class manages reading from the journal.
 */
@NotThreadSafe
public interface JournalReader extends Closeable {
  /**
   * Reads an entry from the journal. Return null if there is no more entry left.
   *
   * @return the journal entry, null if no more entry left
   * @throws InvalidJournalEntryException if the journal entry is invalid (e.g. corrupted entry)
   */
  JournalEntry read() throws IOException, InvalidJournalEntryException;

  /**
   * Gets the the sequence number of the next journal log entry to read. This method is valid
   * no matter whether this JournalReader is closed or not.
   *
   * @return the next sequence number
   */
  long getNextSequenceNumber();
}
