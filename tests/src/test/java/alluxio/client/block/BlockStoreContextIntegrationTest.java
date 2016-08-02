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

package alluxio.client.block;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.resource.CloseableResource;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for {@link BlockStoreContext}.
 */
public final class BlockStoreContextIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();

  /**
   * This test ensures acquiring all the available BlockStore master clients blocks further
   * requests for clients. It also ensures clients are available for reuse after they are released
   * by the previous owners. If the test takes longer than 10 seconds, a deadlock most likely
   * occurred preventing the release of the master clients.
   */
  @Test(timeout = 10000)
  public void acquireMasterLimitTest() throws Exception {
    final List<CloseableResource<BlockMasterClient>> clients = new ArrayList<>();

    // Acquire all the clients
    for (int i = 0; i < Configuration.getInt(Constants.USER_BLOCK_MASTER_CLIENT_THREADS); i++) {
      clients.add(BlockStoreContext.INSTANCE.acquireMasterClientResource());
    }

    // Spawn another thread to acquire a master client
    Thread acquireThread = new Thread(new AcquireMasterClient());
    acquireThread.start();

    // Wait for the spawned thread to complete. If it is able to acquire a master client before
    // the defined timeout, fail
    long timeoutMs = Constants.SECOND_MS / 2;
    long start = System.currentTimeMillis();
    acquireThread.join(timeoutMs);
    if (System.currentTimeMillis() - start < timeoutMs) {
      Assert.fail("Acquired a master client when the client pool was full.");
    }

    // Release all the clients
    for (CloseableResource<BlockMasterClient> client : clients) {
      client.close();
    }

    // Wait for the spawned thread to complete. If it is unable to acquire a master client before
    // the defined timeout, fail.
    timeoutMs = 5 * Constants.SECOND_MS;
    start = System.currentTimeMillis();
    acquireThread.join(timeoutMs);
    if (System.currentTimeMillis() - start >= timeoutMs) {
      Assert.fail("Failed to acquire a master client within " + timeoutMs + "ms. Deadlock?");
    }
  }

  class AcquireMasterClient implements Runnable {
    @Override
    public void run() {
      CloseableResource<BlockMasterClient> client =
          BlockStoreContext.INSTANCE.acquireMasterClientResource();
      client.close();
    }
  }

  /**
   * This test ensures acquiring all the available BlockStore worker clients blocks further requests
   * for clients. It also ensures clients are available for reuse after they are released by the
   * previous owners. If the test takes longer than 10 seconds, a deadlock most likely occurred
   * preventing the release of the worker clients.
   */
  @Test(timeout = 10000)
  @LocalAlluxioClusterResource.Config(
      confParams = {Constants.USER_BLOCK_WORKER_CLIENT_THREADS, "10"})
  public void acquireWorkerLimitTest() throws Exception {
    final List<BlockWorkerClient> clients = new ArrayList<>();

    // Acquire all the clients
    for (int i = 0; i < Configuration.getInt(Constants.USER_BLOCK_WORKER_CLIENT_THREADS); i++) {
      clients.add(BlockStoreContext.INSTANCE.acquireLocalWorkerClient());
    }

    // Spawn another thread to acquire a worker client
    Thread acquireThread = new Thread(new AcquireWorkerClient());
    acquireThread.start();

    // Wait for the spawned thread to complete. If it is able to acquire a worker client before
    // the defined timeout, fail
    long timeoutMs = Constants.SECOND_MS / 2;
    long start = System.currentTimeMillis();
    acquireThread.join(timeoutMs);
    if (System.currentTimeMillis() - start < timeoutMs) {
      Assert.fail("Acquired a worker client when the client pool was full.");
    }

    // Release all the clients
    // Set the RPC number of retries to -1 to prevent the worker client from trying to send a
    // heartbeat message when it is released.
    for (BlockWorkerClient client : clients) {
      BlockStoreContext.INSTANCE.releaseWorkerClient(client);
    }

    // Wait for the spawned thread to complete. If it is unable to acquire a worker client before
    // the defined timeout, fail.
    timeoutMs = 5 * Constants.SECOND_MS;
    start = System.currentTimeMillis();
    acquireThread.join(timeoutMs);
    if (System.currentTimeMillis() - start >= timeoutMs) {
      Assert.fail("Failed to acquire a worker client within " + timeoutMs + "ms. Deadlock?");
    }
  }

  class AcquireWorkerClient implements Runnable {
    @Override
    public void run() {
      BlockWorkerClient client = BlockStoreContext.INSTANCE.acquireLocalWorkerClient();
      BlockStoreContext.INSTANCE.releaseWorkerClient(client);
    }
  }

  @Test
  public void hasLocalWorkerTest() throws Exception {
    Assert.assertTrue(BlockStoreContext.INSTANCE.hasLocalWorker());
  }
}
