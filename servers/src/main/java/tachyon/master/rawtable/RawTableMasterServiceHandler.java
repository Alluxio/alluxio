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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.thrift.TException;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;

import tachyon.TachyonURI;
import tachyon.exception.TachyonException;
import tachyon.retry.CacheUtils;
import tachyon.thrift.RawTableInfo;
import tachyon.thrift.RawTableMasterService;
import tachyon.thrift.TachyonTException;
import tachyon.thrift.ThriftIOException;

public class RawTableMasterServiceHandler implements RawTableMasterService.Iface {
  private final RawTableMaster mRawTableMaster;
  private final Cache<String, Object> replayCache = CacheUtils.createCache();

  public RawTableMasterServiceHandler(RawTableMaster rawTableMaster) {
    mRawTableMaster = rawTableMaster;
  }

  // TODO(jiri) Reduce exception handling boilerplate here
  @Override
  public long createRawTable(String path, int columns, ByteBuffer metadata)
      throws TachyonTException, ThriftIOException {
    try {
      return mRawTableMaster.createRawTable(new TachyonURI(path), columns, metadata);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public long createTable(final String path, final int columns, final ByteBuffer metadata, String nonce)
      throws TachyonTException, ThriftIOException, TException {
    try {
      return ((Long) replayCache.get(nonce, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          return createRawTable(path, columns, metadata);
        }
      }));
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
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
