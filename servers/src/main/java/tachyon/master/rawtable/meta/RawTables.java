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

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.collections.IndexedSet;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TableDoesNotExistException;
import tachyon.master.journal.JournalCheckpointStreamable;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.rawtable.RawTableMaster;
import tachyon.util.io.BufferUtils;

/**
 * All RawTables managed by {@link RawTableMaster}.
 */
public class RawTables implements JournalCheckpointStreamable {
  private final IndexedSet.FieldIndex<RawTable> mIdIndex = new IndexedSet.FieldIndex<RawTable>() {
    @Override
    public Object getFieldValue(RawTable o) {
      return o.getId();
    }
  };
  /** A set of TableInfo indexed by table id */
  // This warning cannot be avoided when passing generics into varargs
  @SuppressWarnings("unchecked")
  private final IndexedSet<RawTable> mTables = new IndexedSet<RawTable>(mIdIndex);

  /**
   * Adds a raw table.
   *
   * @param tableId the id of the raw table
   * @param columns the number of columns in the raw table
   * @param metadata the additional metadata of the raw table
   * @return true if the raw table does not exist before and successfully added, false otherwise
   */
  public synchronized boolean add(long tableId, int columns, ByteBuffer metadata) {
    if (mTables.contains(mIdIndex, tableId)) {
      return false;
    }
    return mTables.add(new RawTable(tableId, columns, metadata));
  }

  /**
   * Removes a raw table.
   *
   * @param tableId the id of the raw table
   * @return true if the table exists and successfully removed, false otherwise
   */
  public synchronized boolean remove(long tableId) {
    return mTables.removeByField(mIdIndex, tableId);
  }

  /**
   * Whether the raw table exists.
   *
   * @param tableId the id of the raw table
   * @return true if the table exists, false otherwise
   */
  public synchronized boolean contains(long tableId) {
    return mTables.contains(mIdIndex, tableId);
  }

  /**
   * Gets the number of the columns of a raw table.
   *
   * @param tableId the id of the raw table
   * @return the number of the columns, -1 if the table does not exist
   */
  public synchronized int getColumns(long tableId) {
    RawTable table = mTables.getFirstByField(mIdIndex, tableId);
    return table == null ? -1 : table.getColumns();
  }

  /**
   * Gets the metadata of the specified raw table.
   *
   * @param tableId The id of the raw table
   * @return null if it has no metadata, or a copy of the internal metadata
   */
  public synchronized ByteBuffer getMetadata(long tableId) {
    RawTable table = mTables.getFirstByField(mIdIndex, tableId);
    return table == null ? null : BufferUtils.cloneByteBuffer(table.getMetadata());
  }

  /**
   * Gets the raw table info.
   *
   * @param tableId the raw table id
   * @return the raw table info if the table exists, null otherwise
   */
  public synchronized RawTable getTableInfo(long tableId) {
    return mTables.getFirstByField(mIdIndex, tableId);
  }

  /**
   * Updates the metadata of the specified raw table. If the new specified metadata is null, the
   * internal metadata will be updated to an empty byte buffer.
   *
   * @param tableId the id of the raw table
   * @param metadata the new metadata of the raw table
   * @throws TableDoesNotExistException when the table does not exist
   */
  public synchronized void updateMetadata(long tableId, ByteBuffer metadata)
      throws TableDoesNotExistException {
    RawTable table = mTables.getFirstByField(mIdIndex, tableId);

    if (table == null) {
      throw new TableDoesNotExistException(
          ExceptionMessage.RAW_TABLE_ID_DOES_NOT_EXIST.getMessage(tableId));
    }

    table.setMetadata(metadata);
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    for (RawTable table : mTables) {
      outputStream.writeEntry(table.toJournalEntry());
    }
  }
}
