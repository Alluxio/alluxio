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

package alluxio.master.keyvalue;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.Constants;
import alluxio.TachyonURI;
import alluxio.exception.TachyonException;
import alluxio.thrift.KeyValueMasterClientService;
import alluxio.thrift.PartitionInfo;
import alluxio.thrift.TachyonTException;
import alluxio.thrift.ThriftIOException;

/**
 * This class is a Thrift handler for key-value master RPCs invoked by a Tachyon client.
 */
@ThreadSafe
public final class KeyValueMasterClientServiceHandler implements KeyValueMasterClientService.Iface {
  private final KeyValueMaster mKeyValueMaster;

  /**
   * Constructs the service handler to process incoming RPC calls for key-value master.
   *
   * @param keyValueMaster handler to the real {@link KeyValueMaster} instance
   */
  public KeyValueMasterClientServiceHandler(KeyValueMaster keyValueMaster) {
    mKeyValueMaster = keyValueMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.KEY_VALUE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public void completePartition(String path, PartitionInfo info) throws TachyonTException {
    try {
      mKeyValueMaster.completePartition(new TachyonURI(path), info);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public void createStore(String path) throws TachyonTException {
    try {
      mKeyValueMaster.createStore(new TachyonURI(path));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public void completeStore(String path) throws TachyonTException {
    try {
      mKeyValueMaster.completeStore(new TachyonURI(path));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public List<PartitionInfo> getPartitionInfo(String path) throws TachyonTException {
    try {
      return mKeyValueMaster.getPartitionInfo(new TachyonURI(path));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public void deleteStore(String path) throws TachyonTException, ThriftIOException {
    try {
      mKeyValueMaster.deleteStore(new TachyonURI(path));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void mergeStore(String fromPath, String toPath)
      throws TachyonTException, ThriftIOException {
    try {
      mKeyValueMaster.mergeStore(new TachyonURI(fromPath), new TachyonURI(toPath));
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }
}
