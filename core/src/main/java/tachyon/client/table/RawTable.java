/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.client.TachyonFS;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.util.CommonUtils;

/**
 * Tachyon provides native support for tables with multiple columns. Each table contains one or
 * more columns. Each columns contains one or more ordered files.
 */
public class RawTable {
  private final TachyonFS TACHYON_CLIENT;
  private final ClientRawTableInfo CLIENT_RAW_TABLE_INFO;

  /**
   * @param tachyonClient
   * @param clientRawTableInfo
   */
  public RawTable(TachyonFS tachyonClient, ClientRawTableInfo clientRawTableInfo) {
    TACHYON_CLIENT = tachyonClient;
    CLIENT_RAW_TABLE_INFO = clientRawTableInfo;
  }

  /**
   * @return the number of columns of the raw table
   */
  public int getColumns() {
    return CLIENT_RAW_TABLE_INFO.getColumns();
  }

  /**
   * @return the id of the raw table
   */
  public int getId() {
    return CLIENT_RAW_TABLE_INFO.getId();
  }

  /**
   * @return the meta data of the raw table
   */
  public ByteBuffer getMetadata() {
    return CommonUtils.cloneByteBuffer(CLIENT_RAW_TABLE_INFO.metadata);
  }

  /**
   * @return the name of the raw table
   */
  public String getName() {
    return CLIENT_RAW_TABLE_INFO.getName();
  }

  /**
   * @return the path of the raw table
   */
  public String getPath() {
    return CLIENT_RAW_TABLE_INFO.getPath();
  }

  /**
   * Get one column of the raw table
   * 
   * @param columnIndex
   *          the index of the column
   * @return the RawColumn
   */
  public RawColumn getRawColumn(int columnIndex) {
    if (columnIndex < 0 || columnIndex >= CLIENT_RAW_TABLE_INFO.getColumns()) {
      CommonUtils.runtimeException(CLIENT_RAW_TABLE_INFO.getPath() + " does not have column "
          + columnIndex + ". It has " + CLIENT_RAW_TABLE_INFO.getColumns() + " columns.");
    }

    return new RawColumn(TACHYON_CLIENT, this, columnIndex);
  }

  /**
   * Update the meta data of the raw table
   * 
   * @param metadata
   *          the new meta data
   * @throws IOException
   */
  public void updateMetadata(ByteBuffer metadata) throws IOException {
    TACHYON_CLIENT.updateRawTableMetadata(CLIENT_RAW_TABLE_INFO.getId(), metadata);
    CLIENT_RAW_TABLE_INFO.setMetadata(CommonUtils.cloneByteBuffer(metadata));
  }
}