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

package tachyon.client;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;

/**
 * Integration tests for <code>tachyon.client.BlockOutStream</code>.
 */
public class BlockOutStreamIntegrationTest {
  private static LocalTachyonCluster sLocalTachyonCluster = null;

  @AfterClass
  public static final void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
  }

  @BeforeClass
  public static final void beforeClass() throws IOException {
    sLocalTachyonCluster = new LocalTachyonCluster(10000, 1000, Constants.GB);
    sLocalTachyonCluster.start();
  }

  /**
   * Returns a local or remote BlockOutStream with a given filename.
   *
   * @param filename The filename for the TachyonFile.
   * @param isLocalWrite If true, a local write will be used.
   * @return a BlockOutStream for the given filename, and specified write location (local/remote).
   * @throws IOException
   */
  private BlockOutStream getBlockOutStream(String filename, boolean isLocalWrite)
      throws IOException {
    TachyonConf conf = sLocalTachyonCluster.getWorkerTachyonConf();
    conf.set(Constants.USER_ENABLE_LOCAL_WRITE, Boolean.toString(isLocalWrite));
    TachyonFS fs = TachyonFS.get(conf);

    TachyonFile file = fs.getFile(fs.createFile(new TachyonURI(filename)));
    return BlockOutStream.get(file, WriteType.MUST_CACHE, 0, conf);
  }

  /**
   * Test disabling local writes.
   *
   * @throws IOException
   */
  @Test
  public void disableLocalWriteTest() throws IOException {
    BlockOutStream os = getBlockOutStream("/file_no_local_write", false);
    Assert.assertTrue(os instanceof RemoteBlockOutStream);
    os.close();
  }

  /**
   * Test enabling local writes.
   *
   * @throws IOException
   */
  @Test
  public void enableLocalWriteTest() throws IOException {
    BlockOutStream os = getBlockOutStream("/file_local_write", true);
    Assert.assertTrue(os instanceof LocalBlockOutStream);
    os.close();
  }

  /**
   * Test cancelling RemoteBlockOutStream.
   *
   * @throws IOException
   */
  @Test
  public void cancelRemoteWriteTest() throws IOException {
    BlockOutStream os = getBlockOutStream("/file_cancel_remote_write", false);
    Assert.assertTrue(os instanceof RemoteBlockOutStream);
    os.cancel();
  }

  /**
   * Test cancelling LocalBlockOutStream.
   *
   * @throws IOException
   */
  @Test
  public void cancelLocalWriteTest() throws IOException {
    BlockOutStream os = getBlockOutStream("/file_cancel_local_write", true);
    Assert.assertTrue(os instanceof LocalBlockOutStream);
    os.cancel();
  }

}
