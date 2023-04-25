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
import alluxio.resource.CloseableIterator;

/**
 * Interface to get an iterator of {@link alluxio.proto.journal.Journal.JournalEntry}s.
 */
public interface JournalEntryIterable {
  /**
   * @return an {@link CloseableIterator} that iterates all the journal entries
   */
  CloseableIterator<Journal.JournalEntry> getJournalEntryIterator();
}
