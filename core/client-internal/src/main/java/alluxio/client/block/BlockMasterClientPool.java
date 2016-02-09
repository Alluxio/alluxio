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

package alluxio.client.block;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.resource.ResourcePool;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for managing block master clients. After obtaining a client with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
final class BlockMasterClientPool extends ResourcePool<BlockMasterClient> {
  private final InetSocketAddress mMasterAddress;

  /**
   * Creates a new block master client pool.
   *
   * @param masterAddress the master address
   */
  public BlockMasterClientPool(InetSocketAddress masterAddress) {
    super(ClientContext.getConf().getInt(Constants.USER_BLOCK_MASTER_CLIENT_THREADS));
    mMasterAddress = masterAddress;
  }

  @Override
  public void close() {
    // TODO(calvin): Consider collecting all the clients and shutting them down.
  }

  @Override
  protected BlockMasterClient createNewResource() {
    return new BlockMasterClient(mMasterAddress, ClientContext.getConf());
  }
}
