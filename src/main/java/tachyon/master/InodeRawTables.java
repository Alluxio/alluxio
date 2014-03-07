/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.io.Utils;
import tachyon.thrift.TachyonException;

public class InodeRawTables {
  // Mapping from inode ID to <Columns, Metadata>
  private Map<Integer, Pair<Integer, ByteBuffer>> mData =
      new HashMap<Integer, Pair<Integer, ByteBuffer>>();

  public synchronized boolean addRawTable(int inodeId, int columns, ByteBuffer metadata)
      throws TachyonException {
    if (mData.containsKey(inodeId)) {
      return false;
    }

    mData.put(inodeId, new Pair<Integer, ByteBuffer>(columns, null));
    updateMetadata(inodeId, metadata);

    return true;
  }

  /**
   * Get the number of the columns of a raw table.
   * 
   * @param inodeId
   *          the inode id of the raw table.
   * @return the number of the columns, -1 if the table does not exist.
   */
  public synchronized int getColumns(int inodeId) {
    Pair<Integer, ByteBuffer> data = mData.get(inodeId);

    return null == data ? -1 : data.getFirst();
  }

  public synchronized ByteBuffer getMetadata(int inodeId) {
    Pair<Integer, ByteBuffer> data = mData.get(inodeId);

    if (null == data) {
      return null;
    }

    ByteBuffer ret = ByteBuffer.allocate(data.getSecond().capacity());
    ret.put(data.getSecond().array());
    ret.flip();

    return ret;
  }

  /**
   * Get the raw table info.
   * 
   * @param inodeId
   *          the raw table id.
   * @return <columns, metadata> if the table exist, null otherwise.
   */
  public synchronized Pair<Integer, ByteBuffer> getTableInfo(int inodeId) {
    return mData.get(inodeId);
  }

  // TODO add version number.
  public synchronized void updateMetadata(int inodeId, ByteBuffer metadata)
      throws TachyonException {
    Pair<Integer, ByteBuffer> data = mData.get(inodeId);

    if (null == data) {
      throw new TachyonException("The raw table " + inodeId + " does not exist.");
    }

    if (metadata == null) {
      data.setSecond(ByteBuffer.allocate(0));
    } else {
      if (metadata.limit() - metadata.position() >= Constants.MAX_TABLE_METADATA_BYTE) {
        throw new TachyonException("Too big table metadata: " + metadata.toString());
      }
      ByteBuffer tMetadata = ByteBuffer.allocate(metadata.limit() - metadata.position());
      tMetadata.put(metadata.array(), metadata.position(), metadata.limit() - metadata.position());
      tMetadata.flip();
      data.setSecond(tMetadata);
    }
  }

  /**
   * Test if the raw table exist or not.
   * 
   * @param inodeId
   *          the raw table id.
   * @return true if the table exists, false otherwise.
   */
  public synchronized boolean exist(int inodeId) {
    return mData.containsKey(inodeId);
  }

  public synchronized void createImageWriter(DataOutputStream os) throws IOException {
    for (Entry<Integer, Pair<Integer, ByteBuffer>> entry : mData.entrySet()) {
      os.writeByte(Image.T_INODE_RAW_TABLE);
      os.writeInt(entry.getKey());
      os.writeInt(entry.getValue().getFirst());
      Utils.writeByteBuffer(entry.getValue().getSecond(), os);
    }
  }

  public synchronized boolean delete(int fileId) {
    mData.remove(fileId);
    return true;
  }
}
