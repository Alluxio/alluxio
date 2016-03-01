/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
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
