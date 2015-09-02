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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.client.next.file.FileInStream;
import tachyon.master.next.LocalTachyonCluster;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for <code>tachyon.client.FileInStream</code>.
 */
public class FileInStreamIntegrationTest {
  private static final int BLOCK_SIZE = 30;
  private static final int MIN_LEN = BLOCK_SIZE + 1;
  private static final int MAX_LEN = 255;
  private static final int MEAN = (MIN_LEN + MAX_LEN) / 2;
  private static final int DELTA = 33;

  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static TachyonFS sTfs = null;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @AfterClass
  public static final void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
  }

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sLocalTachyonCluster = new LocalTachyonCluster(Constants.GB, 1000, BLOCK_SIZE);
    sLocalTachyonCluster.start();
    sTfs = sLocalTachyonCluster.getOldClient();
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
        Assert.assertTrue(is instanceof FileInStream);
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
        Assert.assertTrue(is instanceof FileInStream);
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
        Assert.assertTrue(is instanceof FileInStream);
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        Assert.assertTrue(is instanceof FileInStream);
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
        Assert.assertTrue(is instanceof FileInStream);
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        Assert.assertTrue(is instanceof FileInStream);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code> for end of file.
   */
  @Test
  public void readEndOfFileTest() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        TachyonFile file = sTfs.getFile(fileId);
        FileInStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        Assert.assertTrue(is instanceof FileInStream);
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
   * Test <code>void seek(long pos)</code>. Validate the expected exception for seeking a negative
   * position.
   *
   * @throws IOException
   */
  @Test
  public void seekExceptionTest1() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        TachyonFile file = sTfs.getFile(fileId);
        FileInStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));

        try {
          is.seek(-1);
        } catch (IOException e) {
          // This is expected
          continue;
        }
        is.close();
        throw new IOException("Except seek IOException");
      }
    }
  }

  /**
   * Test <code>void seek(long pos)</code>. Validate the expected exception for seeking a position
   * that is past EOF.
   *
   * @throws IOException
   */
  @Test
  public void seekExceptionTest2() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Seek position is past EOF");
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        TachyonFile file = sTfs.getFile(fileId);
        FileInStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));

        is.seek(k + 1);
        is.close();
      }
    }
  }

  /**
   * Test <code>void seek(long pos)</code>.
   *
   * @throws IOException
   */
  @Test
  public void seekTest() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        TachyonFile file = sTfs.getFile(fileId);
        FileInStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));

        Assert.assertTrue(is instanceof FileInStream);
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
  public void skipTest() throws IOException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        int fileId =
            TachyonFSTestUtils.createByteFile(sTfs, uniqPath + "/file_" + k + "_" + op, op, k);

        TachyonFile file = sTfs.getFile(fileId);
        FileInStream is =
            (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        Assert.assertTrue(is instanceof FileInStream);
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();

        is = (k < MEAN ? file.getInStream(ReadType.CACHE) : file.getInStream(ReadType.NO_CACHE));
        Assert.assertTrue(is instanceof FileInStream);
        Assert.assertEquals(k / 3, is.skip(k / 3));
        Assert.assertEquals(k / 3, is.read());
        is.close();
      }
    }
  }
}
