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

package tachyon.client.concurrent;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.client.ClientContext;
import tachyon.client.StreamOptionUtils;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.util.io.PathUtils;

/**
 * Tests the concurrency of {@link FileInStream}.
 */
public final class FileInStreamConcurrencyIntegrationTest {
  private static final int BLOCK_SIZE = 30;
  private static final int READ_THREADS_NUM =
      ClientContext.getConf().getInt(Constants.USER_BLOCK_MASTER_CLIENT_THREADS) * 10;

  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.GB, Constants.KB, BLOCK_SIZE);
  private static TachyonFileSystem sTfs = null;
  private static TachyonConf sTachyonConf;
  private static OutStreamOptions sWriteTachyon;

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sTfs = sLocalTachyonClusterResource.get().getClient();
    sTachyonConf = sLocalTachyonClusterResource.get().getMasterTachyonConf();
    sWriteTachyon = StreamOptionUtils.getOutStreamOptionsWriteTachyon(sTachyonConf);
  }

  /**
   * Tests the concurrent read of {@link FileInStream}.
   */
  @Test
  public void FileInStreamConcurrencyTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile f =
        TachyonFSTestUtils.createByteFile(sTfs, uniqPath, BLOCK_SIZE * 2, sWriteTachyon);

    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < READ_THREADS_NUM; i ++) {
      threads.add(new Thread(new FileRead(f)));
    }

    ConcurrencyTestUtils.assertConcurrent(threads, 100);
  }

  class FileRead implements Runnable {
    private final TachyonFile mFile;

    FileRead(TachyonFile file) {
      mFile = file;
    }

    @Override
    public void run() {
      try {
        FileInStream stream = sTfs.getInStream(mFile);
        stream.read();
        stream.close();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }
}
