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

import tachyon.thrift.TachyonException;
import tachyon.util.io.BufferUtils;

public class RawTable {
  /** Table ID */
  private final int mId;
  /** Maximum number of columns */
  private final long mMaxColumns;
  /** Number of columns */
  private final int mColumns;
  /** Table metadata */
  private ByteBuffer mMetadata;

  private RawTable(int id, int columns, long maxColumns) {
    mId = id;
    mColumns = columns;
    mMaxColumns = maxColumns;
    mMetadata = null;
  }

  /**
   * Create a new RawTable with metadata set to null. metadata can later be set via
   * {@link #setMetadata(java.nio.ByteBuffer)}.
   *
   * @param id table id
   * @param columns number of columns
   * @param maxColumns maximum number of columns
   */
  public static RawTable newRawTable(int id, int columns, long maxColumns) {
    return new RawTable(id, columns, maxColumns);
  }

  /**
   * Create a new RawTable with metadata set to a ByteBuffer.
   *
   * @param id table id
   * @param columns number of columns
   * @param maxColumns maximum number of columns
   * @param metadata table metadata, if is null, the internal metadata is set to an empty buffer,
   *        otherwise, the provided buffer will be copied into the internal buffer.
   * @return the created RawTable
   * @throws TachyonException when metadata is larger than the configured maximum size
   */
  public static RawTable newRawTable(int id, int columns, long maxColumns, ByteBuffer metadata)
      throws TachyonException {
    RawTable ret = new RawTable(id, columns, maxColumns);
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
   * an empty byte buffer, otherwise, the provided metadata will be copied into the internal buffer.
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
    if (metadata.limit() - metadata.position() >= mMaxColumns) {
      throw new TachyonException("Too big table metadata: " + metadata.toString());
    }
  }
}
