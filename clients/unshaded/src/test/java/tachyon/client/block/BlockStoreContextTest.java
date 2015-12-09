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

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.client.ClientContext;

/**
 * Tests {@link BlockStoreContext}.
 */
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
  public void acquireAtMaxLimitTest() throws Exception {
    final List<BlockMasterClient> clients = Lists.newArrayList();

    // Acquire all the clients
    for (int i = 0; i < ClientContext.getConf()
        .getInt(Constants.USER_BLOCK_MASTER_CLIENT_THREADS); i ++) {
      clients.add(BlockStoreContext.INSTANCE.acquireMasterClient());
    }

    // Spawn another thread to acquire a master client
    Thread acquireThread = new Thread(new AcquireClient());
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

  class AcquireClient implements Runnable {
    @Override
    public void run() {
      BlockMasterClient client = BlockStoreContext.INSTANCE.acquireMasterClient();
      BlockStoreContext.INSTANCE.releaseMasterClient(client);
    }
  }
}
