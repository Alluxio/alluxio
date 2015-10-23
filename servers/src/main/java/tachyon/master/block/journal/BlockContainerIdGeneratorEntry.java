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

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

/**
 * This class represents a journal entry for block container id generator.
 */
public class BlockContainerIdGeneratorEntry extends JournalEntry {
  private final long mNextContainerId;

  /**
   * Creates a new instance of {@link BlockContainerIdGeneratorEntry}.
   *
   * @param nextContainerId the next container id
   */
  @JsonCreator
  public BlockContainerIdGeneratorEntry(
      @JsonProperty("nextContainerId") long nextContainerId) {
    mNextContainerId = nextContainerId;
  }

  /**
   * @return the next container id
   */
  @JsonGetter
  public long getNextContainerId() {
    return mNextContainerId;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.BLOCK_CONTAINER_ID_GENERATOR;
  }
}
