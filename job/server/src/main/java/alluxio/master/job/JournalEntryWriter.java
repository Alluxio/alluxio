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

package alluxio.master.job;

import alluxio.proto.journal.Journal.JournalEntry;

/**
 * An interface for a class which writes journal entries.
 */
public interface JournalEntryWriter {

  /**
   * Writes an entry to a checkpoint file.
   *
   * The entry should not have its sequence number set. This method will add the proper sequence
   * number to the passed in entry.
   *
   * @param entry an entry to write to a journal checkpoint file
   */
  void writeJournalEntry(JournalEntry entry);
}
