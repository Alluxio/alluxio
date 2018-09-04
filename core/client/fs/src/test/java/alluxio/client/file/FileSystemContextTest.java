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

package alluxio.client.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;

import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.Subject;
import java.util.ArrayList;
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
   */
  @Test(timeout = 10000)
  public void acquireAtMaxLimit() throws Exception {
    final List<FileSystemMasterClient> clients = new ArrayList<>();

    // Acquire all the clients
    for (int i = 0; i < Configuration.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS); i++) {
      clients.add(FileSystemContext.get().acquireMasterClient());
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
      FileSystemContext.get().releaseMasterClient(client);
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

  @Test
  public void getCache() throws Exception {
    FileSystemContext ctx1 = FileSystemContext.get();
    FileSystemContext ctx2 = FileSystemContext.get();
    assertEquals(ctx1, ctx2);
  }

  @Test
  public void getDifferentSubjects() throws Exception {
    Subject sub = new Subject();
    FileSystemContext ctx1 = FileSystemContext.get();
    FileSystemContext ctx2 = FileSystemContext.get(sub);
    FileSystemContext ctx3 = FileSystemContext.get(sub);
    assertNotSame(ctx1, ctx2);
    assertSame(ctx2, ctx3);
  }

  @Test
  public void refCount() throws Exception {
    FileSystemContext ctx1 = FileSystemContext.get();
    FileSystemContext ctx2 = FileSystemContext.get();
    ctx1.close();
    FileSystemContext ctx3 = FileSystemContext.get();
    assertSame(ctx1, ctx3); // Same context
    assertSame(ctx2, ctx3); // Same context
    ctx2.close();
    ctx3.close(); // All references closed, so context should be destroyed
    FileSystemContext ctx4 = FileSystemContext.get();
    assertNotSame(ctx1, ctx4); // Different context
  }

  class AcquireClient implements Runnable {
    @Override
    public void run() {
      FileSystemMasterClient client = FileSystemContext.get().acquireMasterClient();
      FileSystemContext.get().releaseMasterClient(client);
    }
  }
}
