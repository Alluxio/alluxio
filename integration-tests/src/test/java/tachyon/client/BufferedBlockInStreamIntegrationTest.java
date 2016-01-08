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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for {@link tachyon.client.block.BlockInStream}.
 */
public class BufferedBlockInStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.GB, Constants.KB, Constants.GB);
  private static TachyonFileSystem sTfs;
  private static TachyonConf sTachyonConf;
  private static OutStreamOptions sWriteBoth;
  private static OutStreamOptions sWriteTachyon;
  private static OutStreamOptions sWriteUnderStore;

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sTfs = sLocalTachyonClusterResource.get().getClient();
    sTachyonConf = sLocalTachyonClusterResource.get().getMasterTachyonConf();
    sWriteBoth = StreamOptionUtils.getOutStreamOptionsWriteBoth(sTachyonConf);
    sWriteTachyon = StreamOptionUtils.getOutStreamOptionsWriteTachyon(sTachyonConf);
    sWriteUnderStore = StreamOptionUtils.getOutStreamOptionsWriteUnderStore(sTachyonConf);
  }

  /**
   * Test {@link tachyon.client.block.BufferedBlockInStream#read()}.
   */
  @Test
  public void readTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        String path = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonFile f = TachyonFSTestUtils.createByteFile(sTfs, path, k, op);

        for (int i = 0; i < 2; i ++) {
          FileInStream is = sTfs.getInStream(f, TachyonFSTestUtils.toInStreamOptions(op));
          byte[] ret = new byte[k];
          int value = is.read();
          int cnt = 0;
          while (value != -1) {
            Assert.assertTrue(value >= 0);
            Assert.assertTrue(value < 256);
            ret[cnt++] = (byte) value;
            value = is.read();
          }
          Assert.assertEquals(cnt, k);
          Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
          is.close();
        }
      }
    }
  }

  /**
   * Test {@link tachyon.client.block.BufferedBlockInStream#read(byte[])}.
   */
  @Test
  public void readTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        String path = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonFile f = TachyonFSTestUtils.createByteFile(sTfs, path, k, op);

        FileInStream is = sTfs.getInStream(f, TachyonFSTestUtils.toInStreamOptions(op));
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = sTfs.getInStream(f, TachyonFSTestUtils.toInStreamOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test {@link tachyon.client.block.BufferedBlockInStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        String path = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonFile f = TachyonFSTestUtils.createByteFile(sTfs, path, k, op);

        FileInStream is = sTfs.getInStream(f, TachyonFSTestUtils.toInStreamOptions(op));

        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = sTfs.getInStream(f, TachyonFSTestUtils.toInStreamOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test {@link tachyon.client.block.BufferedBlockInStream#skip(long)}.
   */
  @Test
  public void skipTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        String path = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonFile f = TachyonFSTestUtils.createByteFile(sTfs, path, k, op);

        FileInStream is = sTfs.getInStream(f, TachyonFSTestUtils.toInStreamOptions(op));

        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();

        is = sTfs.getInStream(f, TachyonFSTestUtils.toInStreamOptions(op));
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
      }
    }
  }

  private List<OutStreamOptions> getOptionSet() {
    List<OutStreamOptions> ret = new ArrayList<OutStreamOptions>(3);
    ret.add(sWriteBoth);
    ret.add(sWriteTachyon);
    ret.add(sWriteUnderStore);
    return ret;
  }
}
