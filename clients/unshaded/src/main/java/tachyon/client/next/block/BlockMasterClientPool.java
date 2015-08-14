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

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import tachyon.client.next.ResourcePool;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.util.ThreadFactoryUtils;

/**
 * Class for managing block master clients. After obtaining a client with Acquire, Release must
 * be called when the thread is done using the client.
 */
public class BlockMasterClientPool extends ResourcePool<MasterClient> {
  private final ExecutorService mExecutorService;

  public BlockMasterClientPool(InetSocketAddress masterAddress, TachyonConf conf) {
    // TODO: Get capacity from conf
    mResources = new LinkedBlockingQueue<MasterClient>(10);
    mExecutorService = Executors.newFixedThreadPool(10, ThreadFactoryUtils.build(
        "block-master-heartbeat-%d", true));

    // Initialize Clients
    for (int i = 0; i < mResources.size(); i ++) {
      mResources.add(new MasterClient(masterAddress, mExecutorService, conf));
    }
  }

  /**
   * Closes the client pool. After this call, the object should be discarded.
   */
  public void close() {
    // TODO: Consider collecting all the clients and shutting them down
    mExecutorService.shutdown();
  }
}
