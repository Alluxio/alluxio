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

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.TachyonException;
import tachyon.util.io.BufferUtils;

public class RawTable {
  private final TachyonConf mTachyonConf;

  /** Table ID */
  private final int mId;
  /** Number of columns */
  private final int mColumns;
  /** Table metadata */
  private ByteBuffer mMetadata;

  private RawTable(TachyonConf tachyonConf, int id, int columns) {
    mTachyonConf = tachyonConf;
    mId = id;
    mColumns = columns;
    mMetadata = null;
  }

  /**
   * Create a new RawTable.
   *
   * @param tachyonConf TachyonConf to use
   * @param id table id
   * @param columns number of columns
   * @param metadata table metadata
   * @return the created RawTable
   * @throws TachyonException when metadata is larger than the configured maximum size
   */
  public static RawTable newRawTable(TachyonConf tachyonConf, int id, int columns,
      ByteBuffer metadata) throws TachyonException {
    RawTable ret = new RawTable(tachyonConf, id, columns);
    ret.setMetadata(metadata);
    return ret;
  }

  public int getId() {
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
   * an empty byte buffer.
   *
   * @param metadata the metadata to be set
   * @throws TachyonException when metadata is larger than the configured maximum size
   */
  public void setMetadata(ByteBuffer metadata) throws TachyonException {
    validateMetadataSize(metadata);
    mMetadata = BufferUtils.cloneByteBuffer(metadata);
  }

  /**
   * Validate size of metadata is smaller than the configured maximum size.
   *
   * @param metadata the metadata to be validated
   * @throws TachyonException if the metadata is too large
   */
  private void validateMetadataSize(ByteBuffer metadata) throws TachyonException {
    long maxSize = mTachyonConf.getBytes(Constants.MAX_TABLE_METADATA_BYTE);
    if (metadata.limit() - metadata.position() >= maxSize) {
      throw new TachyonException("Too big table metadata: " + metadata.toString());
    }
  }
}
