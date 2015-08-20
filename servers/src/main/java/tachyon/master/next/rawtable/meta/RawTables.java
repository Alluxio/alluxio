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

package tachyon.master.next.rawtable.meta;

import java.nio.ByteBuffer;

import tachyon.conf.TachyonConf;
import tachyon.master.next.IndexedSet;
import tachyon.thrift.TachyonException;
import tachyon.util.io.BufferUtils;

/**
 * All RawTables managed by RawTableMaster.
 */
public class RawTables {
  private final IndexedSet.FieldIndex<RawTable> mIdIndex = new IndexedSet.FieldIndex<RawTable>() {
    @Override
    public Object getFieldValue(RawTable o) {
      return o.getId();
    }
  };
  /** A set of TableInfo indexed by table id */
  private final IndexedSet<RawTable> mTables = new IndexedSet<RawTable>(mIdIndex);

  private final TachyonConf mTachyonConf;

  public RawTables(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
  }

  /**
   * Add a raw table.
   *
   * @param tableId The id of the raw table
   * @param columns The number of columns in the raw table
   * @param metadata The additional metadata of the raw table
   * @return true if the raw table does not exist before and successfully added, false otherwise
   * @throws TachyonException when metadata size is larger than maximum configured size
   */
  public synchronized boolean add(int tableId, int columns, ByteBuffer metadata)
      throws TachyonException {
    if (mTables.contains(mIdIndex, tableId)) {
      return false;
    }
    mTables.add(RawTable.newRawTable(mTachyonConf, tableId, columns, metadata));
    return true;
  }

  /**
   * Remove a raw table.
   *
   * @param tableId The id of the raw table
   * @return true if the table exists and successfully removed, false otherwise
   */
  public synchronized boolean remove(int tableId) {
    return mTables.removeByField(mIdIndex, tableId);
  }

  /**
   * Whether the raw table exists.
   *
   * @param tableId the id of the raw table
   * @return true if the table exists, false otherwise.
   */
  public synchronized boolean contains(int tableId) {
    return mTables.contains(mIdIndex, tableId);
  }

  /**
   * Get the number of the columns of a raw table.
   *
   * @param tableId the id of the raw table
   * @return the number of the columns, -1 if the table does not exist.
   */
  public synchronized int getColumns(int tableId) {
    RawTable table = mTables.getFirstByField(mIdIndex, tableId);
    return null == table ? -1 : table.getColumns();
  }

  /**
   * Get the metadata of the specified raw table.
   *
   * @param tableId The id of the raw table
   * @return null if it has no metadata, or a copy of the internal metadata
   */
  public synchronized ByteBuffer getMetadata(int tableId) {
    RawTable table = mTables.getFirstByField(mIdIndex, tableId);
    return null == table ? null : BufferUtils.cloneByteBuffer(table.getMetadata());
  }

  /**
   * Get the raw table info.
   *
   * @param tableId the raw table id.
   * @return the raw table info if the table exists, null otherwise.
   */
  public synchronized RawTable getTableInfo(int tableId) {
    return mTables.getFirstByField(mIdIndex, tableId);
  }

  /**
   * Update the metadata of the specified raw table. If the new specified metadata is null, the
   * internal metadata will be updated to an empty byte buffer.
   *
   * @param tableId The id of the raw table
   * @param metadata The new metadata of the raw table
   * @throws TachyonException when the table does not exist or the metadata size is larger than the
   *         configured maximum size
   */
  public synchronized void updateMetadata(int tableId, ByteBuffer metadata)
      throws TachyonException {
    RawTable table = mTables.getFirstByField(mIdIndex, tableId);

    if (null == table) {
      throw new TachyonException("The raw table " + tableId + " does not exist.");
    }

    table.setMetadata(metadata);
  }
}
