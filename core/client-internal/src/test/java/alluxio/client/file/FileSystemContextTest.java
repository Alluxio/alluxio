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

package alluxio.client.file;

import alluxio.Constants;
import alluxio.client.ClientContext;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link FileSystemContext}.
 */
public final class FileSystemContextTest {
  /**
   * This test ensures acquiring all the available FileSystem master clients blocks further
   * requests for clients. It also ensures clients are available for reuse after they are released
   * by the previous owners. If the test takes longer than 10 seconds, a deadlock most likely
   * occurred preventing the release of the master clients.
   *
   * @throws Exception if an unexpected error occurs during the test
   */
  @Test(timeout = 10000)
  public void acquireAtMaxLimitTest() throws Exception {
    final List<FileSystemMasterClient> clients = Lists.newArrayList();

    // Acquire all the clients
    for (int i = 0; i < ClientContext.getConf()
        .getInt(Constants.USER_FILE_MASTER_CLIENT_THREADS); i++) {
      clients.add(FileSystemContext.INSTANCE.acquireMasterClient());
    }
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
    for (FileSystemMasterClient client : clients) {
      FileSystemContext.INSTANCE.releaseMasterClient(client);
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
      FileSystemMasterClient client = FileSystemContext.INSTANCE.acquireMasterClient();
      FileSystemContext.INSTANCE.releaseMasterClient(client);
    }
  }
}
