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

package tachyon.client.next.block;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.common.base.Throwables;
import tachyon.client.next.ClientOptions;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoWorkerException;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Tachyon Block Store client. This is an internal client for all block level operations in
 * Tachyon. An instance of this class can be obtained via {@link TachyonBS#get}. This class
 * is thread safe.
 */
public class TachyonBS implements Closeable {

  public static TachyonBS sCachedClient = null;

  public static synchronized TachyonBS get(InetSocketAddress masterAddress, TachyonConf conf) {
    if (sCachedClient != null) {
      return sCachedClient;
    }
    sCachedClient = new TachyonBS(masterAddress, conf);
    return sCachedClient;
  }

  private final BlockMasterClientPool mMasterClientPool;
  private final BlockWorkerClientPool mWorkerClientPool;
  private final TachyonConf mTachyonConf;

  public TachyonBS(InetSocketAddress masterAddress, TachyonConf conf) {
    mMasterClientPool = new BlockMasterClientPool(masterAddress, conf);

    // Get the worker address
    // TODO: Simplify this, and use worker master client
    NetAddress workerNetAddress;
    String localHostName = NetworkAddressUtils.getLocalHostName(conf);
    MasterClient masterClient = mMasterClientPool.acquire();
    try {
      workerNetAddress = masterClient.user_getWorker(false, localHostName);
    } catch (NoWorkerException nwe) {
      workerNetAddress = null;
    } catch (IOException ioe) {
      workerNetAddress = null;
    }
    if (null == workerNetAddress) {
      try {
        workerNetAddress = masterClient.user_getWorker(true, "");
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }

    mWorkerClientPool = new BlockWorkerClientPool(workerNetAddress, conf);
    mTachyonConf = conf;
  }

  public void close() {
    mMasterClientPool.close();
    mWorkerClientPool.close();
  }

  public void delete(long blockId) {
    MasterClient masterClient = mMasterClientPool.acquire();
    try {
      // TODO: Implement delete RPC
    } finally {
      mMasterClientPool.release(masterClient);
    }
  }

  public void free(long blockId) {
    // TODO: Implement me
  }

  public FileBlockInfo getInfo(long blockId) throws IOException {
    MasterClient masterClient = mMasterClientPool.acquire();
    try {
      return masterClient.user_getClientBlockInfo(blockId);
    } finally {
      mMasterClientPool.release(masterClient);
    }
  }

  public BlockInStream getInStream(long blockId, ClientOptions options) throws IOException {
    // TODO: Implement me
    return null;
  }

  public BlockOutStream getOutStream(long blockId, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public boolean promote(long blockId) {
    // TODO: Implement me
    return false;
  }
}
