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

package alluxio.client.concurrent;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.AlluxioURI;
import alluxio.client.ClientContext;
import alluxio.client.StreamOptionUtils;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.Configuration;
import alluxio.util.io.PathUtils;

/**
 * Tests the concurrency of {@link FileInStream}.
 */
public final class FileInStreamConcurrencyIntegrationTest {
  private static final int BLOCK_SIZE = 30;
  private static final int READ_THREADS_NUM =
      ClientContext.getConf().getInt(Constants.USER_BLOCK_MASTER_CLIENT_THREADS) * 10;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(Constants.GB, BLOCK_SIZE);
  private static FileSystem sFileSystem = null;
  private static Configuration sConfiguration;
  private static CreateFileOptions sWriteTachyon;

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
    sConfiguration = sLocalAlluxioClusterResource.get().getMasterConf();
    sWriteTachyon = StreamOptionUtils.getCreateFileOptionsMustCache(sConfiguration);
  }

  /**
   * Tests the concurrent read of {@link FileInStream}.
   */
  @Test
  public void FileInStreamConcurrencyTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileSystemTestUtils.createByteFile(sFileSystem, uniqPath, BLOCK_SIZE * 2, sWriteTachyon);

    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < READ_THREADS_NUM; i ++) {
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
      try {
        FileInStream stream = sFileSystem.openFile(mUri);
        stream.read();
        stream.close();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }
}
