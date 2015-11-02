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
 * This class represents a journal entry for setting the pinned flag.
 */
public class SetPinnedEntry extends JournalEntry {
  private final long mId;
  private final boolean mPinned;
  private final long mOpTimeMs;

    /**
     * Creates a new instance of {@link SetPinnedEntry}.
     *
     * @param id the id
     * @param pinned the pinned flag
     * @param opTimeMs the operation time (in milliseconds)
     */
  @JsonCreator
  public SetPinnedEntry(
      @JsonProperty("id") long id,
      @JsonProperty("pinned") boolean pinned,
      @JsonProperty("opTimeMs") long opTimeMs) {
    mId = id;
    mPinned = pinned;
    mOpTimeMs = opTimeMs;
  }

  /**
   * @return the id
   */
  @JsonGetter
  public long getId() {
    return mId;
  }

  /**
   * @return the pinned flag
   */
  @JsonGetter
  public boolean getPinned() {
    return mPinned;
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
    return JournalEntryType.SET_PINNED;
  }

}
