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

package alluxio.client.concurrent;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.options.CreateFileOptions;
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
public final class FileInStreamConcurrencyIntegrationTest extends BaseIntegrationTest {
  private static final int BLOCK_SIZE = 30;
  private static int sNumReadThreads =
      Configuration.getInt(PropertyKey.USER_BLOCK_MASTER_CLIENT_THREADS) * 10;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private static FileSystem sFileSystem = null;
  private static CreateFileOptions sWriteAlluxio;

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
    sWriteAlluxio = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
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

    ConcurrencyTestUtils.assertConcurrent(threads, 100);
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
