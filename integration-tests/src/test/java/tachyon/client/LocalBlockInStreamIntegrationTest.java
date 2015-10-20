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
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for <code>tachyon.client.LocalBlockInStream</code>.
 */
public class LocalBlockInStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static TachyonFileSystem sTfs = null;
  private static OutStreamOptions sWriteBoth;
  private static OutStreamOptions sWriteNative;
  private static InStreamOptions sReadNoCache;
  private static InStreamOptions sReadCache;
  private static TachyonConf sTachyonConf;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @AfterClass
  public static final void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
  }

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sLocalTachyonCluster = new LocalTachyonCluster(Constants.MB, Constants.KB, Constants.GB);
    sLocalTachyonCluster.start();
    sTfs = sLocalTachyonCluster.getClient();
    sTachyonConf = sLocalTachyonCluster.getMasterTachyonConf();
    sWriteBoth =
        new OutStreamOptions.Builder(sTachyonConf).setNativeStorageType(NativeStorageType.STORE)
            .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
    sWriteNative =
        new OutStreamOptions.Builder(sTachyonConf).setNativeStorageType(NativeStorageType.STORE)
            .setUnderStorageType(UnderStorageType.NO_PERSIST).build();
    sReadCache =
        new InStreamOptions.Builder(sTachyonConf).setNativeStorageType(NativeStorageType.STORE)
            .build();
    sReadNoCache =
        new InStreamOptions.Builder(sTachyonConf)
            .setNativeStorageType(NativeStorageType.NO_STORE).build();
  }

  /**
   * Test <code>void read()</code>.
   */
  @Test
  public void readTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        TachyonFile f =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, k, op);

        FileInStream is = sTfs.getInStream(f, sReadNoCache);
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
        Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);

        is = sTfs.getInStream(f, sReadCache);
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
        Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Test <code>void read(byte[] b)</code>.
   */
  @Test
  public void readTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        TachyonFile f =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, k, op);

        FileInStream is = sTfs.getInStream(f, sReadNoCache);
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);

        is = sTfs.getInStream(f, sReadCache);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>.
   */
  @Test
  public void readTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        TachyonFile f =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, k, op);

        FileInStream is = sTfs.getInStream(f, sReadNoCache);
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();
        Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);

        is = sTfs.getInStream(f, sReadCache);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Test <code>void seek(long pos)</code>. Validate the expected exception for seeking a negative
   * position.
   *
   * @throws IOException
   * @throws TException
   */
  @Test
  public void seekExceptionTest1() throws IOException, TachyonException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("Seek position is negative: -1");
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        TachyonFile f =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, k, op);

        FileInStream is = sTfs.getInStream(f, sReadNoCache);

        try {
          is.seek(-1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Test <code>void seek(long pos)</code>. Validate the expected exception for seeking a position
   * that is past buffer limit.
   *
   * @throws IOException
   * @throws TException
   */
  @Test
  public void seekExceptionTest2() throws IOException, TachyonException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("Seek position past end of file: 1");

    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        TachyonFile f =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, k, op);

        FileInStream is = sTfs.getInStream(f, sReadNoCache);
        try {
          is.seek(k + 1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Test <code>void seek(long pos)</code>.
   *
   * @throws IOException
   * @throws TException
   */
  @Test
  public void seekTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        TachyonFile f =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, k, op);

        FileInStream is = sTfs.getInStream(f, sReadNoCache);

        is.seek(k / 3);
        Assert.assertEquals(k / 3, is.read());
        is.seek(k / 2);
        Assert.assertEquals(k / 2, is.read());
        is.seek(k / 4);
        Assert.assertEquals(k / 4, is.read());
        is.close();
      }
    }
  }

  /**
   * Test <code>long skip(long len)</code>.
   */
  @Test
  public void skipTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        TachyonFile f =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, k, op);

        FileInStream is = sTfs.getInStream(f, sReadNoCache);
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();
        Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);

        is = sTfs.getInStream(f, sReadCache);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
        Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);
      }
    }
  }

  private List<OutStreamOptions> getOptionSet() {
    List<OutStreamOptions> ret = new ArrayList<OutStreamOptions>(3);
    ret.add(sWriteBoth);
    ret.add(sWriteNative);
    return ret;
  }
}
