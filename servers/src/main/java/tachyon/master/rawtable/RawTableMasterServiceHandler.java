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

package tachyon.master.rawtable;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.exception.TachyonException;
import tachyon.replay.ReplayCache;
import tachyon.thrift.RawTableInfo;
import tachyon.thrift.RawTableMasterService;
import tachyon.thrift.RpcOptions;
import tachyon.thrift.TachyonTException;
import tachyon.thrift.ThriftIOException;

public class RawTableMasterServiceHandler implements RawTableMasterService.Iface {
  private final RawTableMaster mRawTableMaster;
  private final ReplayCache<Object> mReplayCacheLong = ReplayCache.newInstance();

  public RawTableMasterServiceHandler(RawTableMaster rawTableMaster) {
    mRawTableMaster = rawTableMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.RAW_TABLE_MASTER_SERVICE_VERSION;
  }

  @Override
  public long createRawTable(RpcOptions rpcOptions, final String path, final int columns,
      final ByteBuffer metadata) throws TachyonTException, ThriftIOException {
    return (Long) mReplayCacheLong.run(rpcOptions.getKey(),
        new ReplayCache.ReplayCallableThrowsIOException<Object>() {
          @Override
          public Long call() throws TachyonException, IOException {
            return mRawTableMaster.createRawTable(new TachyonURI(path), columns, metadata);
          }
        });
  }

  @Override
  public long getRawTableId(String path) throws TachyonTException {
    try {
      return mRawTableMaster.getRawTableId(new TachyonURI(path));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public RawTableInfo getClientRawTableInfoById(long id) throws TachyonTException {
    try {
      return mRawTableMaster.getClientRawTableInfo(id);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public RawTableInfo getClientRawTableInfoByPath(String path) throws TachyonTException {
    try {
      return mRawTableMaster.getClientRawTableInfo(new TachyonURI(path));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public void updateRawTableMetadata(long tableId, ByteBuffer metadata) throws TachyonTException {
    try {
      mRawTableMaster.updateRawTableMetadata(tableId, metadata);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }
}
