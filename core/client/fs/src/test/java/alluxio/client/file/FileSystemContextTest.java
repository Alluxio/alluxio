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

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.DeletePOptions;

import com.google.common.io.Closer;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link FileSystemContext}.
 */
public final class FileSystemContextTest {

  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Before
  public void before() {
    mConf = ConfigurationTestUtils.defaults();
  }

  /**
   * This test ensures acquiring all the available FileSystem master clients blocks further
   * requests for clients. It also ensures clients are available for reuse after they are released
   * by the previous owners. If the test takes longer than 10 seconds, a deadlock most likely
   * occurred preventing the release of the master clients.
   */
  @Test(timeout = 10000)
  public void acquireAtMaxLimit() throws Exception {
    Closer closer = Closer.create();

    // Acquire all the clients
    FileSystemContext fsContext = FileSystemContext.create(
        ClientContext.create(mConf));
    for (int i = 0; i < mConf.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX); i++) {
      closer.register(fsContext.acquireMasterClientResource());
    }
    Thread acquireThread = new Thread(new AcquireClient(fsContext));
    acquireThread.start();

    // Wait for the spawned thread to complete. If it is able to acquire a master client before
    // the defined timeout, fail
    long timeoutMs = Constants.SECOND_MS / 2;
    long start = System.currentTimeMillis();
    acquireThread.join(timeoutMs);
    if (System.currentTimeMillis() - start < timeoutMs) {
      fail("Acquired a master client when the client pool was full.");
    }

    // Release all the clients
    closer.close();

    // Wait for the spawned thread to complete. If it is unable to acquire a master client before
    // the defined timeout, fail.
    timeoutMs = 5 * Constants.SECOND_MS;
    start = System.currentTimeMillis();
    acquireThread.join(timeoutMs);
    if (System.currentTimeMillis() - start >= timeoutMs) {
      fail("Failed to acquire a master client within " + timeoutMs + "ms. Deadlock?");
    }
  }

  class AcquireClient implements Runnable {

    private final FileSystemContext mFsCtx;

    public AcquireClient(FileSystemContext fsContext) {
      mFsCtx = fsContext;
    }

    @Override
    public void run() {
      CloseableResource<FileSystemMasterClient> client = mFsCtx.acquireMasterClientResource();
      client.close();
    }
  }

  @Test
  public void excessWorkerGroupTest() throws Exception {
    ArrayList<Thread> clients = new ArrayList<>();
    int numClients = 25;
    int numFiles = 10000;
    int totalData = 50 * Constants.MB; // 50MB

    int perFileSize = totalData / numClients / numFiles;
    byte[] data = new byte[perFileSize];
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
    String dirPrefix = "test";
    FileSystem fs = FileSystem.Factory.create(conf);
    try {
      fs.delete(new AlluxioURI("/" + dirPrefix),
          DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).build());
    } catch (FileDoesNotExistException e) {
      // ok
    }

    for (int i = 0; i < numClients; i++) {
      fs = FileSystem.Factory.create(conf);
      String dir = String.format("/%s/", dirPrefix) + i;
      clients.add(new Thread(new CreateOp(fs, numFiles, data, dir)));
    }
    long startTime = System.currentTimeMillis();
    clients.forEach(Thread::start);
    clients.forEach((client) -> {
      try {
        client.join();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    long endTime = System.currentTimeMillis();
    System.out.println("Runtime: " + (endTime - startTime) + " millis");
  }

  class CreateOp implements Runnable {
    private final int mNumFiles;
    private final FileSystem mFs;
    private final byte[] mData;
    private final String mDir;

    public CreateOp(FileSystem fs, int numFiles, byte[] data, String dir) {
      mFs = fs;
      mNumFiles = numFiles;
      mData = data;
      mDir = dir;
    }

    @Override
    public void run() {
      try {
        try {
          mFs.delete(new AlluxioURI(mDir), DeletePOptions.newBuilder().setRecursive(true).build());
        } catch (FileDoesNotExistException e) {
          // ok to continue
        }
        mFs.createDirectory(new AlluxioURI(mDir),
            CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
        for (int i = 0; i < mNumFiles; i++) {
          try (FileOutStream out = mFs.createFile(new AlluxioURI(mDir + "/" + i))) {
            out.write(mData);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
