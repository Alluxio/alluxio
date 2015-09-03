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

package tachyon.client.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import tachyon.client.TachyonFS;
import tachyon.thrift.RawTableInfo;
import tachyon.util.io.BufferUtils;

/**
 * Tachyon provides native support for tables with multiple columns. Each table contains one or more
 * columns. Each columns contains one or more ordered files.
 */
public class RawTable {
  private final TachyonFS mTachyonFS;
  private final RawTableInfo mRawTableInfo;

  /**
   * Creates a new <code>RawTable</code>
   *
   * @param tachyonClient the <code>TachyonFS</code> client
   * @param rawTableInfo information describing the table
   */
  public RawTable(TachyonFS tachyonClient, RawTableInfo rawTableInfo) {
    mTachyonFS = tachyonClient;
    mRawTableInfo = rawTableInfo;
  }

  /**
   * @return the number of columns of the raw table
   */
  public int getColumns() {
    return mRawTableInfo.getColumns();
  }

  /**
   * @return the id of the raw table
   */
  public int getId() {
    return mRawTableInfo.getId();
  }

  /**
   * @return the meta data of the raw table
   */
  public ByteBuffer getMetadata() {
    return BufferUtils.cloneByteBuffer(mRawTableInfo.metadata);
  }

  /**
   * @return the name of the raw table
   */
  public String getName() {
    return mRawTableInfo.getName();
  }

  /**
   * @return the path of the raw table
   */
  public String getPath() {
    return mRawTableInfo.getPath();
  }

  /**
   * Get one column of the raw table.
   *
   * @param columnIndex the index of the column
   * @return the RawColumn
   */
  public RawColumn getRawColumn(int columnIndex) {
    Preconditions.checkArgument(columnIndex >= 0 && columnIndex < mRawTableInfo.getColumns(),
        mRawTableInfo.getPath() + " does not have column " + columnIndex + ". It has "
            + mRawTableInfo.getColumns() + " columns.");

    return new RawColumn(mTachyonFS, this, columnIndex);
  }

  /**
   * Update the meta data of the raw table
   *
   * @param metadata the new meta data
   * @throws IOException if an event that prevents the metadata from being updated is encountered.
   */
  public void updateMetadata(ByteBuffer metadata) throws IOException {
    mTachyonFS.updateRawTableMetadata(mRawTableInfo.getId(), metadata);
    mRawTableInfo.setMetadata(BufferUtils.cloneByteBuffer(metadata));
  }
}
