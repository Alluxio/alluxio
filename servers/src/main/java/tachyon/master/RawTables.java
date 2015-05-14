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

package tachyon.master;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectWriter;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.conf.TachyonConf;
import tachyon.io.Utils;
import tachyon.thrift.TachyonException;

/**
 * All Raw Table related info in MasterInfo.
 */
public class RawTables extends ImageWriter {
  // Mapping from table id to <Columns, Metadata>
  private Map<Integer, Pair<Integer, ByteBuffer>> mData =
      new HashMap<Integer, Pair<Integer, ByteBuffer>>();

  private final TachyonConf mTachyonConf;

  public RawTables(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
  }

  /**
   * Add a raw table. It will check if the raw table is already added.
   *
   * @param tableId The id of the raw table
   * @param columns The number of columns in the raw table
   * @param metadata The additional metadata of the raw table
   * @return true if success, false otherwise
   * @throws TachyonException
   */
  public synchronized boolean addRawTable(int tableId, int columns, ByteBuffer metadata)
      throws TachyonException {
    if (mData.containsKey(tableId)) {
      return false;
    }

    mData.put(tableId, new Pair<Integer, ByteBuffer>(columns, null));
    updateMetadata(tableId, metadata);

    return true;
  }

  /**
   * Remove a raw table.
   *
   * @param tableId The id of the raw table
   * @return true if success, false otherwise
   */
  public synchronized boolean delete(int tableId) {
    mData.remove(tableId);
    return true;
  }

  /**
   * Test if the raw table exist or not.
   *
   * @param inodeId the raw table id.
   * @return true if the table exists, false otherwise.
   */
  public synchronized boolean exist(int inodeId) {
    return mData.containsKey(inodeId);
  }

  /**
   * Get the number of the columns of a raw table.
   *
   * @param tableId the inode id of the raw table.
   * @return the number of the columns, -1 if the table does not exist.
   */
  public synchronized int getColumns(int tableId) {
    Pair<Integer, ByteBuffer> data = mData.get(tableId);

    return null == data ? -1 : data.getFirst();
  }

  /**
   * Get the metadata of the specified raw table. It will return a duplication.
   *
   * @param tableId The id of the raw table
   * @return null if it has no metadata, or a duplication of the metadata
   */
  public synchronized ByteBuffer getMetadata(int tableId) {
    Pair<Integer, ByteBuffer> data = mData.get(tableId);

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
   * @param tableId the raw table id.
   * @return <columns, metadata> if the table exist, null otherwise.
   */
  public synchronized Pair<Integer, ByteBuffer> getTableInfo(int tableId) {
    return mData.get(tableId);
  }

  /**
   * Load the image into the RawTables structure.
   *
   * @param ele the json element to load
   * @throws IOException
   */
  void loadImage(ImageElement ele) throws IOException {
    List<Integer> ids = ele.get("ids", new TypeReference<List<Integer>>() {});
    List<Integer> columns = ele.get("columns", new TypeReference<List<Integer>>() {});
    List<ByteBuffer> data = ele.getByteBufferList("data");

    for (int k = 0; k < ids.size(); k ++) {
      try {
        if (!addRawTable(ids.get(k), columns.get(k), data.get(k))) {
          throw new IOException("Failed to create raw table");
        }
      } catch (TachyonException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Update the metadata of the specified raw table. It will check if the table exists.
   *
   * @param tableId The id of the raw table
   * @param metadata The new metadata of the raw table
   * @throws TachyonException
   */
  // TODO add version number.
  public synchronized void updateMetadata(int tableId, ByteBuffer metadata)
      throws TachyonException {
    Pair<Integer, ByteBuffer> data = mData.get(tableId);

    if (null == data) {
      throw new TachyonException("The raw table " + tableId + " does not exist.");
    }

    if (metadata == null) {
      data.setSecond(ByteBuffer.allocate(0));
    } else {
      long maxVal = mTachyonConf.getBytes(Constants.MAX_TABLE_METADATA_BYTE, 0L);
      if (metadata.limit() - metadata.position() >= maxVal) {
        throw new TachyonException("Too big table metadata: " + metadata.toString());
      }
      ByteBuffer tMetadata = ByteBuffer.allocate(metadata.limit() - metadata.position());
      tMetadata.put(metadata.array(), metadata.position(), metadata.limit() - metadata.position());
      tMetadata.flip();
      data.setSecond(tMetadata);
    }
  }

  @Override
  public synchronized void writeImage(ObjectWriter objWriter, DataOutputStream dos)
      throws IOException {
    List<Integer> ids = new ArrayList<Integer>();
    List<Integer> columns = new ArrayList<Integer>();
    List<ByteBuffer> data = new ArrayList<ByteBuffer>();
    for (Entry<Integer, Pair<Integer, ByteBuffer>> entry : mData.entrySet()) {
      ids.add(entry.getKey());
      columns.add(entry.getValue().getFirst());
      data.add(entry.getValue().getSecond());
    }

    ImageElement ele =
        new ImageElement(ImageElementType.RawTable).withParameter("ids", ids)
            .withParameter("columns", columns)
            .withParameter("data", Utils.byteBufferListToBase64(data));

    writeElement(objWriter, dos, ele);
  }
}
