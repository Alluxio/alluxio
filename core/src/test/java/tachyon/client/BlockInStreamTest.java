/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.master.LocalTachyonCluster;

/**
 * Unit tests for <code>tachyon.client.BlockInStream</code>.
 */
public class BlockInStreamTest {
  private final int MIN_LEN = 0;
  private final int MAX_LEN = 255;
  private final int MEAN = (MIN_LEN + MAX_LEN) / 2;
  private final int DELTA = 33;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  /**
   * Test <code>void read()</code>.
   */
  @Test
  public void readTest1() throws IOException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId = TestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mTfs.getFile(fileId);
        InStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        if (k == 0) {
          Assert.assertTrue(is instanceof EmptyBlockInStream);
        } else {
          Assert.assertTrue(is instanceof BlockInStream);
        }
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
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        if (k == 0) {
          Assert.assertTrue(is instanceof EmptyBlockInStream);
        } else {
          Assert.assertTrue(is instanceof BlockInStream);
        }
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
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test <code>void read(byte b[])</code>.
   */
  @Test
  public void readTest2() throws IOException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId = TestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mTfs.getFile(fileId);
        InStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        if (k == 0) {
          Assert.assertTrue(is instanceof EmptyBlockInStream);
        } else {
          Assert.assertTrue(is instanceof BlockInStream);
        }
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        if (k == 0) {
          Assert.assertTrue(is instanceof EmptyBlockInStream);
        } else {
          Assert.assertTrue(is instanceof BlockInStream);
        }
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>.
   */
  @Test
  public void readTest3() throws IOException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId = TestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mTfs.getFile(fileId);
        InStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        if (k == 0) {
          Assert.assertTrue(is instanceof EmptyBlockInStream);
        } else {
          Assert.assertTrue(is instanceof BlockInStream);
        }
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        if (k == 0) {
          Assert.assertTrue(is instanceof EmptyBlockInStream);
        } else {
          Assert.assertTrue(is instanceof BlockInStream);
        }
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test <code>long skip(long len)</code>.
   */
  @Test
  public void skipTest() throws IOException {
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId = TestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mTfs.getFile(fileId);
        InStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        Assert.assertTrue(is instanceof BlockInStream);
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        Assert.assertTrue(is instanceof BlockInStream);
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
