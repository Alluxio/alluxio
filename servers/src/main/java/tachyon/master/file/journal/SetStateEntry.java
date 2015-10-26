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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import tachyon.client.file.options.SetStateOptions;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SetStateEntry extends JournalEntry {
  private final long mId;
  private final long mOpTimeMs;
  private final Boolean mPinned;
  private final Long mTTL;

  /**
   * Creates a new instance of <code>SetStateEntry</code>.
   *
   * @param id  the id of the entry
   * @param opTimeMs the operation timestamp (in millisecs)
   * @param pinned the pinned flag to be set, otherwise, null
   * @param ttl the new TTL value to be set, otherwise, null
   */
  @JsonCreator
  public SetStateEntry(@JsonProperty("id") long id, @JsonProperty("operationTimeMs") long opTimeMs,
      @JsonProperty("pinned") Boolean pinned, @JsonProperty("ttl") Long ttl) {
    mId = id;
    mOpTimeMs = opTimeMs;
    mPinned = pinned;
    mTTL = ttl;
  }

  public SetStateEntry(long id, long opTimeMs, SetStateOptions options) {
    this(id, opTimeMs, options.getPinned().orNull(), options.getTTL().orNull());
  }

  @JsonGetter
  public long getId() {
    return mId;
  }

  @JsonGetter
  public long getOperationTimeMs() {
    return mOpTimeMs;
  }

  @JsonGetter
  public Boolean getPinned() {
    return mPinned;
  }

  @JsonGetter
  public Long getTTL() {
    return mTTL;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.SET_STATE;
  }
}
