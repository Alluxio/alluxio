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

package tachyon.master.rawtable.journal;

import java.nio.ByteBuffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

/**
 * This class represents a journal entry for a raw table.
 */
public class RawTableEntry extends JournalEntry {
  private final long mId;
  private final int mColumns;
  private final ByteBuffer mMetadata;

  /**
   * Creates a new instance of {@link RawTableEntry}.
   *
   * @param id table id
   * @param columns the columns to be set for the table
   * @param metadata the metadata to be set for the table
   */
  @JsonCreator
  public RawTableEntry(
      @JsonProperty("id") long id,
      @JsonProperty("columns") int columns,
      @JsonProperty("metadata") ByteBuffer metadata) {
    mId = id;
    mColumns = columns;
    mMetadata = metadata;
  }

  /**
   * @return the id
   */
  @JsonGetter
  public long getId() {
    return mId;
  }

  /**
   * @return the columns
   */
  @JsonGetter
  public int getColumns() {
    return mColumns;
  }

  /**
   * @return the metadata
   */
  @JsonGetter
  public ByteBuffer getMetadata() {
    return mMetadata;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.RAW_TABLE;
  }
}
