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

package tachyon.client.block;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import tachyon.client.BlockMasterClient;
import tachyon.client.ClientContext;
import tachyon.resource.ResourcePool;
import tachyon.util.ThreadFactoryUtils;

/**
 * Class for managing block master clients. After obtaining a client with
 * {@link ResourcePool#acquire}, {@link ResourcePool#release} must be called when the thread is done
 * using the client.
 */
public class BlockMasterClientPool extends ResourcePool<BlockMasterClient> {
  private static final int CAPACITY = 10;
  private final ExecutorService mExecutorService;
  private final InetSocketAddress mMasterAddress;

  /**
   * Creates a new block master client pool.
   *
   * @param masterAddress the master address
   */
  public BlockMasterClientPool(InetSocketAddress masterAddress) {
    // TODO(calvin): Get the capacity from configuration.
    super(CAPACITY);
    mExecutorService =
        Executors.newFixedThreadPool(CAPACITY,
            ThreadFactoryUtils.build("block-master-heartbeat-%d", true));
    mMasterAddress = masterAddress;
  }

  @Override
  public void close() {
    // Collecting all the clients and shutting them down.
    ArrayList<BlockMasterClient> masterClients = new ArrayList<BlockMasterClient>();
    if (this.mResources.drainTo(masterClients) > 0) {
      for (BlockMasterClient masterClient : masterClients) {
        masterClient.close();
      }
    }
    mExecutorService.shutdown();
  }

  @Override
  protected BlockMasterClient createNewResource() {
    return new BlockMasterClient(mMasterAddress, mExecutorService, ClientContext.getConf());
  }
}
