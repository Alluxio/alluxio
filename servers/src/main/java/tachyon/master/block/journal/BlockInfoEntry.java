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

package tachyon.master.block.journal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

/**
 * The class represents a journal entry for block information.
 */
public class BlockInfoEntry extends JournalEntry {
  private final long mBlockId;
  private final long mLength;

  /**
   * Creates a new instance of {@link BlockInfoEntry}.
   *
   * @param blockId the block id
   * @param length the length
   */
  @JsonCreator
  public BlockInfoEntry(
      @JsonProperty("blockId") long blockId,
      @JsonProperty("length") long length) {
    mBlockId = blockId;
    mLength = length;
  }

  /**
   * @return the block id
   */
  @JsonGetter
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the length
   */
  @JsonGetter
  public long getLength() {
    return mLength;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.BLOCK_INFO;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockInfoEntry)) {
      return false;
    }
    BlockInfoEntry that = (BlockInfoEntry) o;
    return Objects.equal(mBlockId, that.mBlockId)
        && Objects.equal(mLength, that.mLength);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId, mLength);
  }

}
