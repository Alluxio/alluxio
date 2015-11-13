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

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.RawTableMasterClient;
import tachyon.exception.TachyonException;
import tachyon.thrift.RawTableInfo;

/**
 * Tachyon Raw Table client. This class should be used to interface with the Tachyon Raw Table
 * master.
 */
@PublicApi
public class AbstractTachyonRawTables implements TachyonRawTablesCore {
  protected RawTablesContext mContext;

  protected AbstractTachyonRawTables() {
    mContext = RawTablesContext.INSTANCE;
  }

  // TODO(calvin): Consider different client options
  @Override
  public SimpleRawTable create(TachyonURI path, int numColumns, ByteBuffer metadata)
      throws IOException, TachyonException {
    RawTableMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long rawTableId = masterClient.createRawTable(path, numColumns, metadata);
      return new SimpleRawTable(rawTableId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public RawTableInfo getInfo(SimpleRawTable rawTable) throws IOException, TachyonException {
    RawTableMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getClientRawTableInfo(rawTable.getRawTableId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public SimpleRawTable open(TachyonURI path) throws IOException, TachyonException {
    RawTableMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long rawTableId = masterClient.getClientRawTableInfo(path).getId();
      return new SimpleRawTable(rawTableId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void updateRawTableMetadata(SimpleRawTable rawTable, ByteBuffer metadata)
      throws IOException, TachyonException {
    RawTableMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.updateRawTableMetadata(rawTable.getRawTableId(), metadata);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
