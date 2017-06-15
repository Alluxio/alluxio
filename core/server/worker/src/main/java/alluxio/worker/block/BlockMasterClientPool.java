/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.resource.ResourcePool;

import com.google.common.io.Closer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for managing block master clients. After obtaining a client with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
public final class BlockMasterClientPool extends ResourcePool<BlockMasterClient> {
  private final InetSocketAddress mMasterAddress;
  private final Queue<BlockMasterClient> mClientList;

  /**
   * Creates a new block master client pool.
   *
   * @param masterAddress the master address
   */
  public BlockMasterClientPool(InetSocketAddress masterAddress) {
    super(Configuration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE));
    mMasterAddress = masterAddress;
    mClientList = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void close() throws IOException {
    BlockMasterClient client;
    Closer closer = Closer.create();
    while ((client = mClientList.poll()) != null) {
      closer.register(client);
    }
    closer.close();
  }

  @Override
  protected BlockMasterClient createNewResource() {
    BlockMasterClient client = new BlockMasterClient(mMasterAddress);
    mClientList.add(client);
    return client;
  }
}
