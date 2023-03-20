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
import alluxio.util.CommonUtils;

/**
 * Journaled component responsible for journaling a single journal entry.
 */
public abstract class SingleEntryJournaled implements Journaled {

  private Journal.JournalEntry mEntry = Journal.JournalEntry.getDefaultInstance();

  @Override
  public CloseableIterator<Journal.JournalEntry> getJournalEntryIterator() {
    return CloseableIterator.noopCloseable(CommonUtils.singleElementIterator(mEntry));
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    if (!Journal.JournalEntry.getDefaultInstance().equals(mEntry)) {
      LOG.warn("{} has already processed an entry", getCheckpointName());
    }
    mEntry = entry;
    return true;
  }

  @Override
  public void resetState() {
    mEntry = Journal.JournalEntry.getDefaultInstance();
  }

  /**
   * @return the entry stored by this object
   */
  public Journal.JournalEntry getEntry() {
    if (Journal.JournalEntry.getDefaultInstance().equals(mEntry)) {
      LOG.warn("{} has not processed any entries", getCheckpointName());
    }
    return mEntry;
  }
}
