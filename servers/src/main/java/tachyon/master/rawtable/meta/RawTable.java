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

package tachyon.master.rawtable.meta;

import java.nio.ByteBuffer;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryRepresentable;
import tachyon.master.rawtable.journal.RawTableEntry;
import tachyon.util.io.BufferUtils;

public class RawTable implements JournalEntryRepresentable {
  /** Table ID */
  private final long mId;
  /** Number of columns */
  private final int mColumns;
  /** Table metadata */
  private ByteBuffer mMetadata;

  /**
   * Create a new RawTable with metadata set to null. metadata can later be set via
   * {@link #setMetadata(java.nio.ByteBuffer)}.
   *
   * @param id table id
   * @param columns number of columns
   */
  public RawTable(long id, int columns) {
    mId = id;
    mColumns = columns;
    mMetadata = null;
  }

  /**
   * Create a new RawTable with metadata set to a ByteBuffer.
   *
   * @param id table id
   * @param columns number of columns
   * @param metadata table metadata, if is null, the internal metadata is set to an empty buffer,
   *        otherwise, the provided buffer will be copied into the internal buffer.
   */
  public RawTable(long id, int columns, ByteBuffer metadata) {
    mId = id;
    mColumns = columns;
    setMetadata(metadata);
  }

  public long getId() {
    return mId;
  }

  public int getColumns() {
    return mColumns;
  }

  public ByteBuffer getMetadata() {
    return mMetadata;
  }

  /**
   * Set the table metadata. If the specified metadata is null, the internal metadata will be set to
   * an empty byte buffer, otherwise, the provided metadata will be copied into the internal buffer.
   *
   * @param metadata the metadata to be set
   */
  public void setMetadata(ByteBuffer metadata) {
    mMetadata = BufferUtils.cloneByteBuffer(metadata);
  }

  @Override
  public JournalEntry toJournalEntry() {
    return new RawTableEntry(mId, mColumns, mMetadata);
  }
}
