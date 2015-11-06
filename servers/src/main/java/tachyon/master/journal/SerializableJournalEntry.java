/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.journal;

import com.fasterxml.jackson.annotation.JsonGetter;

import tachyon.Constants;

/**
 * A {@link JournalEntry} with a sequence number.
 */
public class SerializableJournalEntry {
  private final long mSequenceNumber;
  private final JournalEntry mEntry;

  protected SerializableJournalEntry(long sequenceNumber, JournalEntry entry) {
    mSequenceNumber = sequenceNumber;
    mEntry = entry;
  }

  /**
   * @return the sequence number for this entry
   */
  @JsonGetter(Constants.JOURNAL_JSON_ENTRY_SEQUENCE_NUMBER_KEY)
  public long getSequenceNumber() {
    return mSequenceNumber;
  }

  @JsonGetter(Constants.JOURNAL_JSON_ENTRY_TYPE_KEY)
  public JournalEntryType getType() {
    return mEntry.getType();
  }

  @JsonGetter(Constants.JOURNAL_JSON_ENTRY_PARAMETER_KEY)
  public JournalEntry getEntry() {
    return mEntry;
  }
}
