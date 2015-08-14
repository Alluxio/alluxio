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
import java.net.InetSocketAddress;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.next.ClientOptions;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;

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

  public TachyonBS(InetSocketAddress masterAddress, TachyonConf conf) {
    mMasterClientPool = new BlockMasterClientPool(masterAddress, conf);
  }

  public void close() {
    mMasterClientPool.close();
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

  public BlockInfo getInfo(long blockId) {
    MasterClient masterClient = mMasterClientPool.acquire();

  }

  public BlockInStream getInStream(long blockId, ClientOptions options) {
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
