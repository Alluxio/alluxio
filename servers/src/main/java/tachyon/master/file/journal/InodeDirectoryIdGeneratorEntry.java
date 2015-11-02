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

package tachyon.master.file.journal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

/**
 * This class represents a journal entry for a directory id generator.
 */
public class InodeDirectoryIdGeneratorEntry extends JournalEntry {
  private final long mContainerId;
  private final long mSequenceNumber;

  /**
   * Creates a new instance of {@link InodeDirectoryIdGeneratorEntry}.
   *
   * @param containerId the container id
   * @param sequenceNumber the sequence number
   */
  @JsonCreator
  public InodeDirectoryIdGeneratorEntry(
      @JsonProperty("containerId") long containerId,
      @JsonProperty("sequenceNumber") long sequenceNumber) {
    mContainerId = containerId;
    mSequenceNumber = sequenceNumber;
  }

  /**
   * @return the container id
   */
  @JsonGetter
  public long getContainerId() {
    return mContainerId;
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
    return JournalEntryType.INODE_DIRECTORY_ID_GENERATOR;
  }

}
