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
import tachyon.client.next.file.FileInStream;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for <code>tachyon.client.BlockInStream</code>.
 */
public class BlockInStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int MEAN = (MIN_LEN + MAX_LEN) / 2;
  private static final int DELTA = 33;

  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static TachyonFS sTfs = null;

  @AfterClass
  public static final void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sLocalTachyonCluster = new LocalTachyonCluster(10000, 1000, Constants.GB);
    sLocalTachyonCluster.start();
    sTfs = sLocalTachyonCluster.getClient();
  }

  /**
   * Test disable local read
   *
   * @throws IOException
   */
  @Test
  public void disableLocalReadTest() throws IOException {
    int fileId =
        TachyonFSTestUtils.createByteFile(sTfs, "/file_no_local_read", WriteType.TRY_CACHE, 10);

    TachyonConf conf = sLocalTachyonCluster.getWorkerTachyonConf();
    conf.set(Constants.USER_ENABLE_LOCAL_READ, "false");
    TachyonFS sTfs1 = TachyonFS.get(conf);
    TachyonFile file1 = sTfs1.getFile(fileId);
    FileInStream is1 = file1.getInStream(ReadType.NO_CACHE);
    is1.close();

    conf.set(Constants.USER_ENABLE_LOCAL_READ, "true");
    TachyonFS sTfs2 = TachyonFS.get(conf);
    TachyonFile file2 = sTfs2.getFile(fileId);
    FileInStream is2 = file2.getInStream(ReadType.NO_CACHE);
    is2.close();
  }

  /**
   * Test <code>void read()</code>.
   */
  @Test
  public void readTest1() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        TachyonFile file = sTfs.getFile(fileId);
        FileInStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
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

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
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

  /**
   * Test <code>void read(byte[] b)</code>.
   */
  @Test
  public void readTest2() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        TachyonFile file = sTfs.getFile(fileId);
        FileInStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>.
   */
  @Test
  public void readTest3() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        TachyonFile file = sTfs.getFile(fileId);
        FileInStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test <code>long skip(long len)</code>.
   */
  @Test
  public void skipTest() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        TachyonFile file = sTfs.getFile(fileId);
        FileInStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
      }
    }
  }
}
