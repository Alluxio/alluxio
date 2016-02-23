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

package alluxio.client.lineage;

import alluxio.Constants;
import alluxio.client.ClientContext;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link LineageContext}.
 */
public final class LineageContextTest {

  /**
   * Tests the concurrency of the {@link LineageContext}.
   *
   * @throws Exception when the thread fails to wait
   */
  @Test
  public void concurrencyTest() throws Exception {
    final List<LineageMasterClient> clients = Lists.newArrayList();

    // acquire all the clients
    for (int i = 0; i < ClientContext.getConf()
        .getInt(Constants.USER_LINEAGE_MASTER_CLIENT_THREADS); i++) {
      clients.add(LineageContext.INSTANCE.acquireMasterClient());
    }

    (new Thread(new AcquireClient())).start();

    // wait for thread to run
    Thread.sleep(5L);

    // release all the clients
    for (LineageMasterClient client : clients) {
      LineageContext.INSTANCE.releaseMasterClient(client);
    }
  }

  class AcquireClient implements Runnable {
    @Override
    public void run() {
      LineageMasterClient client = LineageContext.INSTANCE.acquireMasterClient();
      LineageContext.INSTANCE.releaseMasterClient(client);
    }
  }
}
