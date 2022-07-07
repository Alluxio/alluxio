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

package alluxio.client.fs.concurrent;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.Configuration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.test.util.ConcurrencyUtils;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.PathUtils;

import com.google.common.base.Throwables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests the concurrency of {@link FileInStream}.
 */
public final class ConcurrentFileInStreamIntegrationTest extends BaseIntegrationTest {
  private static final int BLOCK_SIZE = 30;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          // Increase this timeout as in this test, there can be thousands of threads connecting to
          // a worker. This may lead to failures when the worker is slow or lack of resource,
          .setProperty(PropertyKey.NETWORK_CONNECTION_AUTH_TIMEOUT, "20s")
          .build();

  private FileSystem mFileSystem;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  /**
   * Tests the concurrent read of {@link FileInStream}.
   */
  @Test
  public void FileInStreamConcurrency() throws Exception {
    int numReadThreads =
        Configuration.getInt(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX) * 10;
    AlluxioURI uniqPath = new AlluxioURI(PathUtils.uniqPath());
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath.getPath(), BLOCK_SIZE * 2,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).setRecursive(true)
            .build());

    List<Runnable> threads = new ArrayList<>();
    for (int i = 0; i < numReadThreads; i++) {
      threads.add(new FileRead(uniqPath));
    }

    ConcurrencyUtils.assertConcurrent(threads, 100);
  }

  class FileRead implements Runnable {
    private final AlluxioURI mUri;

    FileRead(AlluxioURI uri) {
      mUri = uri;
    }

    @Override
    public void run() {
      try (FileInStream stream = mFileSystem.openFile(mUri)) {
        stream.read();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }
}
