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
import org.junit.rules.Timeout;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for {@link tachyon.client.file.FileInStream}.
 */
public class FileInStreamIntegrationTest {
  private static final int BLOCK_SIZE = 30;
  private static final int MIN_LEN = BLOCK_SIZE + 1;
  private static final int MAX_LEN = BLOCK_SIZE * 10 + 1;
  private static final int DELTA = BLOCK_SIZE / 2;

  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.GB, Constants.KB, BLOCK_SIZE);
  private static FileSystem sTfs = null;
  private static TachyonConf sTachyonConf;
  private static CreateFileOptions sWriteBoth;
  private static CreateFileOptions sWriteTachyon;
  private static CreateFileOptions sWriteUnderStore;

  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(60);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sTfs = sLocalTachyonClusterResource.get().getClient();
    sTachyonConf = sLocalTachyonClusterResource.get().getMasterTachyonConf();
    sWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough(sTachyonConf);
    sWriteTachyon = StreamOptionUtils.getCreateFileOptionsMustCache(sTachyonConf);
    sWriteUnderStore = StreamOptionUtils.getCreateFileOptionsThrough(sTachyonConf);
  }

  /**
   * Test {@link FileInStream#read()} across block boundary.
   */
  @Test
  public void readTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonURI uri = new TachyonURI(filename);
        TachyonFSTestUtils.createByteFile(sTfs, filename, k, op);

        FileInStream is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
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

        is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
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
   * Test {@link FileInStream#read(byte[])}.
   */
  @Test
  public void readTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonURI uri = new TachyonURI(filename);
        TachyonFSTestUtils.createByteFile(sTfs, filename, k, op);

        FileInStream is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test {@link FileInStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonURI uri = new TachyonURI(filename);
        TachyonFSTestUtils.createByteFile(sTfs, filename, k, op);

        FileInStream is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test {@link FileInStream#read(byte[], int, int)} for end of file.
   */
  @Test
  public void readEndOfFileTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonURI uri = new TachyonURI(filename);
        TachyonFSTestUtils.createByteFile(sTfs, filename, k, op);

        FileInStream is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
        try {
          byte[] ret = new byte[k / 2];
          int readBytes = is.read(ret, 0, k / 2);
          while (readBytes != -1) {
            readBytes = is.read(ret);
            Assert.assertTrue(0 != readBytes);
          }
          Assert.assertEquals(-1, readBytes);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Test {@link FileInStream#seek(long)}. Validate the expected exception for seeking a negative
   * position.
   *
   * @throws IOException
   * @throws TachyonException
   */
  @Test
  public void seekExceptionTest1() throws IOException, TachyonException {
    mThrown.expect(IllegalArgumentException.class);
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonURI uri = new TachyonURI(filename);
        TachyonFSTestUtils.createByteFile(sTfs, filename, k, op);

        FileInStream is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
        try {
          is.seek(-1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Test {@link FileInStream#seek(long)}. Validate the expected exception for seeking a position
   * that is past EOF.
   *
   * @throws IOException
   */
  @Test
  public void seekExceptionTest2() throws IOException, TachyonException {
    mThrown.expect(IllegalArgumentException.class);
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonURI uri = new TachyonURI(filename);
        TachyonFSTestUtils.createByteFile(sTfs, filename, k, op);

        FileInStream is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
        try {
          is.seek(k + 1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Test {@link FileInStream#seek(long)}.
   *
   * @throws IOException
   * @throws TachyonException
   */
  @Test
  public void seekTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonURI uri = new TachyonURI(filename);
        TachyonFSTestUtils.createByteFile(sTfs, filename, k, op);

        FileInStream is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
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
   * Test {@link FileInStream#seek(long)} when at the end of a file at the block boundary.
   *
   * @throws IOException
   */
  @Test
  public void eofSeekTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    int length = BLOCK_SIZE * 3;
    for (CreateFileOptions op : getOptionSet()) {
      String filename = uniqPath + "/file_" + op.hashCode();
      TachyonURI uri = new TachyonURI(filename);
      TachyonFSTestUtils.createByteFile(sTfs, filename, length, op);

      FileInStream is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
      byte[] data = new byte[length];
      is.read(data, 0, length);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(length, data));
      is.seek(0);
      is.read(data, 0, length);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(length, data));
      is.close();
    }
  }

  /**
   * Test {@link FileInStream#skip(long)}.
   */
  @Test
  public void skipTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = uniqPath + "/file_" + k + "_" + op.hashCode();
        TachyonURI uri = new TachyonURI(filename);
        TachyonFSTestUtils.createByteFile(sTfs, filename, k, op);

        FileInStream is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();

        is = sTfs.openFile(uri, TachyonFSTestUtils.toOpenFileOptions(op));
        Assert.assertEquals(k / 3, is.skip(k / 3));
        Assert.assertEquals(k / 3, is.read());
        is.close();
      }
    }
  }

  private List<CreateFileOptions> getOptionSet() {
    List<CreateFileOptions> ret = new ArrayList<CreateFileOptions>(3);
    ret.add(sWriteBoth);
    ret.add(sWriteTachyon);
    ret.add(sWriteUnderStore);
    return ret;
  }
}
