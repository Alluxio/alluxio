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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.worker.BlockWorkerClient;
import tachyon.thrift.WorkerInfo;
import tachyon.thrift.WorkerNetAddress;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Tests {@link BlockStoreContext}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class, BlockMasterClientPool.class, BlockStoreContext.class,
    BlockWorkerClient.class, BlockWorkerClientPool.class})
public final class BlockStoreContextTest {
  /**
   * This test ensures acquiring all the available BlockStore master clients blocks further
   * requests for clients. It also ensures clients are available for reuse after they are released
   * by the previous owners. If the test takes longer than 10 seconds, a deadlock most likely
   * occurred preventing the release of the master clients.
   *
   * @throws Exception if an unexpected error occurs during the test
   */
  @Test(timeout = 10000)
  public void acquireMasterLimitTest() throws Exception {
    final List<BlockMasterClient> clients = Lists.newArrayList();

    // Acquire all the clients
    for (int i = 0; i < ClientContext.getConf()
        .getInt(Constants.USER_BLOCK_MASTER_CLIENT_THREADS); i ++) {
      clients.add(BlockStoreContext.INSTANCE.acquireMasterClient());
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
    for (BlockMasterClient client : clients) {
      BlockStoreContext.INSTANCE.releaseMasterClient(client);
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
      BlockMasterClient client = BlockStoreContext.INSTANCE.acquireMasterClient();
      BlockStoreContext.INSTANCE.releaseMasterClient(client);
    }
  }

  /**
   * This test ensures acquiring all the available BlockStore worker clients blocks further requests
   * for clients. It also ensures clients are available for reuse after they are released by the
   * previous owners. If the test takes longer than 10 seconds, a deadlock most likely occurred
   * preventing the release of the worker clients.
   *
   * @throws Exception if an unexpected error occurs during the test
   */
  @Test(timeout = 10000)
  public void acquireWorkerLimitTest() throws Exception {
    // Use mocks for the master client to make sure the pool of local block worker clients is
    // initialized properly.
    Whitebox.setInternalState(NetworkAddressUtils.class, "sLocalHost", "localhost");
    BlockMasterClient masterClientMock = PowerMockito.mock(BlockMasterClient.class);
    List<WorkerInfo> list = Lists.newArrayList();
    list.add(new WorkerInfo(0, new WorkerNetAddress("localhost", 0, 0, 0), 0, "", 0, 0, 0));
    PowerMockito.doReturn(list).when(masterClientMock).getWorkerInfoList();
    PowerMockito.whenNew(BlockMasterClient.class).withArguments(Mockito.any(), Mockito.any())
        .thenReturn(masterClientMock);

    // Use mocks for the block worker client to prevent it from trying to invoke the session
    // heartbeat RPC.
    BlockWorkerClient workerClientMock = PowerMockito.mock(BlockWorkerClient.class);
    PowerMockito.doNothing().when(workerClientMock).sessionHeartbeat();
    PowerMockito.doReturn(true).when(workerClientMock).isLocal();
    PowerMockito
        .whenNew(BlockWorkerClient.class)
        .withArguments(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong(),
            Mockito.anyBoolean(), Mockito.any()).thenReturn(workerClientMock);

    final List<BlockWorkerClient> clients = Lists.newArrayList();

    // Acquire all the clients
    for (int i = 0; i < ClientContext.getConf()
        .getInt(Constants.USER_BLOCK_WORKER_CLIENT_THREADS); i ++) {
      clients.add(BlockStoreContext.INSTANCE.acquireWorkerClient());
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
      BlockWorkerClient client = BlockStoreContext.INSTANCE.acquireWorkerClient();
      BlockStoreContext.INSTANCE.releaseWorkerClient(client);
    }
  }
}
