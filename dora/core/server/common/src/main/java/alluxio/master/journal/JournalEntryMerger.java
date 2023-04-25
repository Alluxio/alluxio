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

import alluxio.proto.journal.Journal;

import java.util.List;

/**
 * The interface for a journal entry merge which merges multiple inode journals on the
 * same inode object into one.
 */
public interface JournalEntryMerger {
  /**
   * Adds a new journal entry.
   * @param entry the new journal entry to add
   */
  void add(Journal.JournalEntry entry);

  /**
   * Returns a list of journal entries which have been merged.
   * @return the merged journal entries
   */
  List<Journal.JournalEntry> getMergedJournalEntries();

  /**
   * Clears the existing journal entries.
   */
  void clear();
}
