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
 * This class represents a journal entry for persisting a file.
 */
public class PersistFileEntry extends JournalEntry {
  private final long mId;
  private final long mLength;
  private final long mOpTimeMs;

  /**
   * Creates a new instance {@link PersistFileEntry}.
   *
   * @param id the id
   * @param length the length
   * @param opTimeMs the operation time (in milliseconds)
   */
  @JsonCreator
  public PersistFileEntry(
      @JsonProperty("id") long id,
      @JsonProperty("length") long length,
      @JsonProperty("opTimeMs") long opTimeMs) {
    mId = id;
    mLength = length;
    mOpTimeMs = opTimeMs;
  }

  /**
   * @return the file id
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
    return JournalEntryType.ADD_CHECKPOINT;
  }
}
