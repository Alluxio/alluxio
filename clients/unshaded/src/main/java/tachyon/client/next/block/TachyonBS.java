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

import tachyon.Constants;
import tachyon.client.next.ClientOptions;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoWorkerException;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.next.WorkerClient;

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

  private final TachyonConf mTachyonConf;

  public TachyonBS(InetSocketAddress masterAddress, TachyonConf conf) {
    mTachyonConf = conf;
  }

  public void close() {

  }

  // TODO: Evaluate if this is necessary for now, or if file level delete is sufficient
  public void delete(long blockId) {
    MasterClient masterClient = mMasterClientPool.acquire();
    try {
      // TODO: Implement delete RPC
    } finally {
      mMasterClientPool.release(masterClient);
    }
  }

  // TODO: Evaluate if this is necessary for now, or if file level free is sufficient
  public void free(long blockId) {
    // TODO: Implement free RPC
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
    WorkerClient workerClient = mWorkerClientPool.acquire();

    try {
      // No specified location to read from
      if (null == options.getLocation()) {
        // Local client, attempt to do direct read from local storage
        if (workerClient.isLocal()) {
          // Try to lock the block, if it succeeds, the data is available locally, otherwise
          // default to using a remote stream
          String localFile = workerClient.lockBlock(blockId);
          if (null != localFile) {
            return new LocalBlockInStream();
          }
        }

        // Client is not local or the data is not available on the local worker, use remote stream
        return new RemoteBlockInStream();
      }

      // TODO: Handle the case when a location is specified
      return null;
    } finally {
      mWorkerClientPool.release(workerClient);
    }
  }

  public BlockOutStream getOutStream(long blockId, ClientOptions options) throws IOException {
    WorkerClient workerClient = mWorkerClientPool.acquire();
    try {
      // No specified location to read from
      if (null == options.getLocation()) {
        // Local client, attempt to do direct write to local storage
        if (workerClient.isLocal()) {
          // TODO: Read the initial size from the configuration
          String localFile = workerClient.requestBlockLocation(blockId, 8 * Constants.MB);
          return new LocalBlockOutStream();
        }

        // Client is not local or the data is not available on the local worker, use remote stream
        return new RemoteBlockOutStream();
      }

      // TODO: Handle the case when a location is specified
      return null;
    } finally {
      mWorkerClientPool.release(workerClient);
    }
  }

  // TODO: Evaluate if this is necessary for now, or if file level promote is sufficient
  public boolean promote(long blockId) {
    // TODO: Implement me
    return false;
  }
}
