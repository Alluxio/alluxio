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

package tachyon.client.next;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.client.next.file.FileInStream;
import tachyon.client.next.file.TachyonFS;
import tachyon.client.next.file.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.master.next.LocalTachyonCluster;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for <code>tachyon.client.FileInStream</code>.
 */
public class FileInStreamIntegrationTest {
  private static final int BLOCK_SIZE = 30;
  private static final int MIN_LEN = BLOCK_SIZE + 1;
  private static final int MAX_LEN = BLOCK_SIZE * 10 + 1;
  private static final int MEAN = (MIN_LEN + MAX_LEN) / 2;
  private static final int DELTA = BLOCK_SIZE / 2;

  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static TachyonFS sTfs = null;
  private static TachyonConf sTachyonConf;
  private static ClientOptions sWriteBoth;
  private static ClientOptions sWriteTachyon;
  private static ClientOptions sWriteUnderStore;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @AfterClass
  public static final void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
  }

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sLocalTachyonCluster = new LocalTachyonCluster(Constants.GB, Constants.KB, BLOCK_SIZE);
    sLocalTachyonCluster.start();
    sTfs = sLocalTachyonCluster.getClient();
    sTachyonConf = sLocalTachyonCluster.getMasterTachyonConf();
    sWriteBoth =
        new ClientOptions.Builder(sTachyonConf).setCacheType(CacheType.CACHE)
            .setUnderStorageType(UnderStorageType.PERSIST).build();
    sWriteTachyon =
        new ClientOptions.Builder(sTachyonConf).setCacheType(CacheType.CACHE)
            .setUnderStorageType(UnderStorageType.NO_PERSIST).build();
    sWriteUnderStore =
        new ClientOptions.Builder(sTachyonConf).setCacheType(CacheType.NO_CACHE)
            .setUnderStorageType(UnderStorageType.PERSIST).build();
  }

  /**
   * Test <code>void read() across block boundary</code>.
   */
  @Test
  public void readTest1() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (ClientOptions op : getOptionSet()) {
        TachyonFile f =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        FileInStream is = sTfs.getInStream(f, op);
        byte[] ret = new byte[k];
        int value = is.read();
        int cnt = 0;
        while (value != -1) {
          Assert.assertTrue(value >= 0);
          Assert.assertTrue(value < 256);
          ret[cnt ++] = (byte) value;
          value = is.read();
        }
        Assert.assertEquals(cnt, k);
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = sTfs.getInStream(f, op);
        ret = new byte[k];
        value = is.read();
        cnt = 0;
        while (value != -1) {
          Assert.assertTrue(value >= 0);
          Assert.assertTrue(value < 256);
          ret[cnt ++] = (byte) value;
          value = is.read();
        }
        Assert.assertEquals(cnt, k);
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  private List<ClientOptions> getOptionSet() {
    List<ClientOptions> ret = new ArrayList<ClientOptions>(3);
    ret.add(sWriteBoth);
    ret.add(sWriteTachyon);
    ret.add(sWriteUnderStore);
    return ret;
  }
}
