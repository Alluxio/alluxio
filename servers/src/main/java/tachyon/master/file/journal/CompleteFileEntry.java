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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

/**
 * This class represents a journal entry for completion of a file.
 */
public final class CompleteFileEntry extends JournalEntry {
  private final List<Long> mBlockIds;
  private final long mId;
  private final long mLength;
  private final long mOpTimeMs;

  /**
   * Creates a new instance of {@link CompleteFileEntry}.
   *
   * @param blockIds the block ids
   * @param id the id
   * @param length the length
   * @param opTimeMs the operation time (in milliseconds)
   */
  @JsonCreator
  public CompleteFileEntry(
      @JsonProperty("blockIds") List<Long> blockIds,
      @JsonProperty("id") long id,
      @JsonProperty("length") long length,
      @JsonProperty("opTimeMs") long opTimeMs) {
    mBlockIds = Preconditions.checkNotNull(blockIds);
    mId = id;
    mLength = length;
    mOpTimeMs = opTimeMs;
  }

  /**
   * @return the block ids
   */
  @JsonGetter
  public List<Long> getBlockIds() {
    return mBlockIds;
  }

  /**
   * @return the id
   */
  @JsonGetter
  public long getId() {
    return mId;
  }

  /**
   * @return the length
   */
  @JsonGetter
  public long getLength() {
    return mLength;
  }

  /**
   * @return the operation time (in milliseconds)
   */
  @JsonGetter
  public long getOpTimeMs() {
    return mOpTimeMs;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.COMPLETE_FILE;
  }
}
