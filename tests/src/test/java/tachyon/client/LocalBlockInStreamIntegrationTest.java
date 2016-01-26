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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.PreconditionMessage;
import tachyon.exception.TachyonException;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for {@link tachyon.client.block.LocalBlockInStream}.
 */
public class LocalBlockInStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.MB, Constants.KB, Constants.MB);
  private static FileSystem sTfs = null;
  private static CreateFileOptions sWriteBoth;
  private static CreateFileOptions sWriteTachyon;
  private static OpenFileOptions sReadNoCache;
  private static OpenFileOptions sReadCache;
  private static TachyonConf sTachyonConf;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sTfs = sLocalTachyonClusterResource.get().getClient();
    sTachyonConf = sLocalTachyonClusterResource.get().getMasterTachyonConf();
    sWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough(sTachyonConf);
    sWriteTachyon = StreamOptionUtils.getCreateFileOptionsMustCache(sTachyonConf);
    sReadCache = StreamOptionUtils.getOpenFileOptionsCache(sTachyonConf);
    sReadNoCache = StreamOptionUtils.getOpenFileOptionsNoCache(sTachyonConf);
  }

  /**
   * Test {@link tachyon.client.block.LocalBlockInStream#read()}.
   */
  @Test
  public void readTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode());
        TachyonFSTestUtils.createByteFile(sTfs, uri, op, k);

        FileInStream is = sTfs.openFile(uri, sReadNoCache);
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
        Assert.assertTrue(sTfs.getStatus(uri).getInMemoryPercentage() == 100);

        is = sTfs.openFile(uri, sReadCache);
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
        Assert.assertTrue(sTfs.getStatus(uri).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Test {@link tachyon.client.block.LocalBlockInStream#read(byte[])}.
   */
  @Test
  public void readTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode());
        TachyonFSTestUtils.createByteFile(sTfs, uri, op, k);

        FileInStream is = sTfs.openFile(uri, sReadNoCache);
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sTfs.getStatus(uri).getInMemoryPercentage() == 100);

        is = sTfs.openFile(uri, sReadCache);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sTfs.getStatus(uri).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Test {@link tachyon.client.block.LocalBlockInStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode());
        TachyonFSTestUtils.createByteFile(sTfs, uri, op, k);

        FileInStream is = sTfs.openFile(uri, sReadNoCache);
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();
        Assert.assertTrue(sTfs.getStatus(uri).getInMemoryPercentage() == 100);

        is = sTfs.openFile(uri, sReadCache);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sTfs.getStatus(uri).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Test {@link tachyon.client.block.LocalBlockInStream#seek(long)}. Validate the expected
   * exception for seeking a negative position.
   *
   * @throws IOException
   * @throws TachyonException
   */
  @Test
  public void seekExceptionTest1() throws IOException, TachyonException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE, -1));
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode());
        TachyonFSTestUtils.createByteFile(sTfs, uri, op, k);

        FileInStream is = sTfs.openFile(uri, sReadNoCache);

        try {
          is.seek(-1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Test {@link tachyon.client.block.LocalBlockInStream#seek(long)}. Validate the expected
   * exception for seeking a position that is past buffer limit.
   *
   * @throws IOException
   * @throws TachyonException
   */
  @Test
  public void seekExceptionTest2() throws IOException, TachyonException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE, 1));

    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode());
        TachyonFSTestUtils.createByteFile(sTfs, uri, op, k);

        FileInStream is = sTfs.openFile(uri, sReadNoCache);
        try {
          is.seek(k + 1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Test {@link tachyon.client.block.LocalBlockInStream#seek(long)}.
   *
   * @throws IOException
   * @throws TachyonException
   */
  @Test
  public void seekTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode());
        TachyonFSTestUtils.createByteFile(sTfs, uri, op, k);

        FileInStream is = sTfs.openFile(uri, sReadNoCache);

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
   * Test {@link tachyon.client.block.LocalBlockInStream#skip(long)}.
   */
  @Test
  public void skipTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode());
        TachyonFSTestUtils.createByteFile(sTfs, uri, op, k);

        FileInStream is = sTfs.openFile(uri, sReadNoCache);
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();
        Assert.assertTrue(sTfs.getStatus(uri).getInMemoryPercentage() == 100);

        is = sTfs.openFile(uri, sReadCache);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
        Assert.assertTrue(sTfs.getStatus(uri).getInMemoryPercentage() == 100);
      }
    }
  }

  private List<CreateFileOptions> getOptionSet() {
    List<CreateFileOptions> ret = new ArrayList<CreateFileOptions>(3);
    ret.add(sWriteBoth);
    ret.add(sWriteTachyon);
    return ret;
  }
}
