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
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.test.util.ConcurrencyUtils;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.PathUtils;

import com.google.common.base.Throwables;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests the concurrency of {@link FileInStream}.
 */
public final class ConcurrentFileInStreamIntegrationTest extends BaseIntegrationTest {
  private static final int BLOCK_SIZE = 30;
  private static int sNumReadThreads =
      ServerConfiguration.getInt(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX) * 10;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private static FileSystem sFileSystem = null;
  private static CreateFilePOptions sWriteAlluxio;

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
    sWriteAlluxio = CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE)
        .setRecursive(true).build();
  }

  /**
   * Tests the concurrent read of {@link FileInStream}.
   */
  @Test
  public void FileInStreamConcurrency() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileSystemTestUtils.createByteFile(sFileSystem, uniqPath, BLOCK_SIZE * 2, sWriteAlluxio);

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < sNumReadThreads; i++) {
      threads.add(new Thread(new FileRead(new AlluxioURI(uniqPath))));
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
      try (FileInStream stream = sFileSystem.openFile(mUri)) {
        stream.read();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }
}
