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

package tachyon.master.lineage.journal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

/**
 * This class represent a journal entry for lineage id generator.
 */
public class LineageIdGeneratorEntry extends JournalEntry {
  private final long mSequenceNumber;

  /**
   * Creates a new instance of {@code LineageIdGeneratorEntry}.
   *
   * @param sequenceNumber the sequence number
   */
  @JsonCreator
  public LineageIdGeneratorEntry(
      @JsonProperty("sequenceNumber") long sequenceNumber) {
    mSequenceNumber = sequenceNumber;
  }

  /**
   * @return the sequence number
   */
  @JsonGetter
  public long getSequenceNumber() {
    return mSequenceNumber;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.LINEAGE_ID_GENERATOR;
  }
}
