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

package tachyon.client.lineage;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.client.ClientContext;

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
        .getInt(Constants.USER_LINEAGE_MASTER_CLIENT_THREADS); i ++) {
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
