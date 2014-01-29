/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.nio.ByteBuffer;

import tachyon.Constants;
import tachyon.thrift.TachyonException;

/**
 * Tachyon file system's RawTable representation in master.
 */
public class InodeRawTable extends InodeFolder {
  protected final int COLUMNS;

  private ByteBuffer mMetadata;

  public InodeRawTable(String name, int id, int parentId, int columns, ByteBuffer metadata,
      long creationTimeMs) throws TachyonException {
    super(name, id, parentId, InodeType.RawTable, creationTimeMs);
    COLUMNS = columns;
    updateMetadata(metadata);
  }

  public int getColumns() {
    return COLUMNS;
  }

  // TODO add version number.
  public synchronized void updateMetadata(ByteBuffer metadata) throws TachyonException {
    if (metadata == null) {
      mMetadata = ByteBuffer.allocate(0);
    } else {
      if (metadata.limit() - metadata.position() >= Constants.MAX_TABLE_METADATA_BYTE) {
        throw new TachyonException("Too big table metadata: " + metadata.toString());
      }
      mMetadata = ByteBuffer.allocate(metadata.limit() - metadata.position());
      mMetadata.put(metadata.array(), metadata.position(), metadata.limit() - metadata.position());
      mMetadata.flip();
    }
  }

  public synchronized ByteBuffer getMetadata() {
    ByteBuffer ret = ByteBuffer.allocate(mMetadata.capacity());
    ret.put(mMetadata.array());
    ret.flip();
    return ret;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeRawTable(");
    sb.append(super.toString()).append(",").append(COLUMNS).append(",");
    sb.append(mMetadata).append(")");
    return sb.toString();
  }
}