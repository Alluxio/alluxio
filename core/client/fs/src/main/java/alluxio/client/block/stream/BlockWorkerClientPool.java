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

package alluxio.client.block.stream;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.resource.ResourcePool;

import com.google.common.io.Closer;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * Class for managing block worker clients. After obtaining a client with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
public final class BlockWorkerClientPool extends ResourcePool<BlockWorkerClient> {
  private final Queue<BlockWorkerClient> mClientList;
  private final BlockWorkerClient.Builder mBuilder;

  /**
   * Creates a new block master client pool.
   *
   * @param subject the parent subject
   * @param address address of the worker
   */
  public BlockWorkerClientPool(Subject subject, SocketAddress address) {
    super(Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_THREADS));
    mBuilder = BlockWorkerClient.getBuilder(subject, address);
    mClientList = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void close() throws IOException {
    BlockWorkerClient client;
    Closer closer = Closer.create();
    while ((client = mClientList.poll()) != null) {
      closer.register(client);
    }
    closer.close();
  }

  @Override
  protected BlockWorkerClient createNewResource() {
    BlockWorkerClient client = mBuilder.build();
    mClientList.add(client);
    return client;
  }
}
