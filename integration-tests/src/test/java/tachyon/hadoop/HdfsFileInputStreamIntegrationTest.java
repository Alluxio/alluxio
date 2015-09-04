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

package tachyon.hadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientFileInfo;
import tachyon.util.io.BufferUtils;

/**
 * Integration tests for HdfsFileInputStream.
 */
public class HdfsFileInputStreamIntegrationTest {
  private static final int USER_QUOTA_UNIT_BYTES = 100;
  private static final int WORKER_CAPACITY = 10000;
  private static final int FILE_LEN = 255;
  private static final int BUFFER_SIZE = 50;

  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static TachyonFS sTFS = null;
  private HdfsFileInputStream mInMemInputStream;
  private HdfsFileInputStream mUfsInputStream;

  @AfterClass
  public static final void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
  }

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY, USER_QUOTA_UNIT_BYTES,
        Constants.GB);
    sLocalTachyonCluster.start();
    sTFS = sLocalTachyonCluster.getClient();
    TachyonFSTestUtils.createByteFile(sTFS, "/testFile1", WriteType.CACHE_THROUGH, FILE_LEN);
    TachyonFSTestUtils.createByteFile(sTFS, "/testFile2", WriteType.THROUGH, FILE_LEN);
  }

  @After
  public final void after() throws Exception {
    mInMemInputStream.close();
    mUfsInputStream.close();
  }

  @Before
  public final void before() throws IOException {
    ClientFileInfo fileInfo = sTFS.getFileStatus(-1, new TachyonURI("/testFile1"));
    mInMemInputStream = new HdfsFileInputStream(sTFS, fileInfo.getId(),
        new Path(fileInfo.getUfsPath()), new Configuration(), BUFFER_SIZE, null,
        sLocalTachyonCluster.getMasterTachyonConf());

    fileInfo = sTFS.getFileStatus(-1, new TachyonURI("/testFile2"));
    mUfsInputStream = new HdfsFileInputStream(sTFS, fileInfo.getId(),
        new Path(fileInfo.getUfsPath()), new Configuration(), BUFFER_SIZE, null,
        sLocalTachyonCluster.getMasterTachyonConf());
  }

  /**
   * Test <code>int read()</code>.
   */
  @Test
  public void readTest1() throws IOException {
    for (int i = 0; i < FILE_LEN; i ++) {
      int value = mInMemInputStream.read();
      Assert.assertEquals(i & 0x00ff, value);
      value = mUfsInputStream.read();
      Assert.assertEquals(i & 0x00ff, value);
    }
    Assert.assertEquals(FILE_LEN, mInMemInputStream.getPos());
    Assert.assertEquals(FILE_LEN, mUfsInputStream.getPos());

    int value = mInMemInputStream.read();
    Assert.assertEquals(-1, value);
    value = mUfsInputStream.read();
    Assert.assertEquals(-1, value);
  }

  /**
   * Test <code>int read(byte[] b, int off, int len)</code>.
   */
  @Test
  public void readTest2() throws IOException {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));

    Arrays.fill(buf, (byte) 0);
    length = mUfsInputStream.read(buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));

    Arrays.fill(buf, (byte) 0);
    length = mInMemInputStream.read(buf, 0, 1);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(buf, 0, 1);
    Assert.assertEquals(-1, length);
  }

  /**
   * Test <code>int read(long position, byte[] buffer, int offset, int length)</code>.
   */
  @Test
  public void readTest3() throws IOException {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(0, buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    Arrays.fill(buf, (byte) 0);
    length = mUfsInputStream.read(0, buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    buf = new byte[FILE_LEN - 10];
    Arrays.fill(buf, (byte) 0);
    length = mInMemInputStream.read(10, buf, 0, FILE_LEN - 10);
    Assert.assertEquals(FILE_LEN - 10, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    Arrays.fill(buf, (byte) 0);
    length = mUfsInputStream.read(10, buf, 0, FILE_LEN - 10);
    Assert.assertEquals(FILE_LEN - 10, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    Arrays.fill(buf, (byte) 0);
    length = mInMemInputStream.read(-1, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(-1, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);

    length = mInMemInputStream.read(FILE_LEN, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(FILE_LEN, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);
  }

  @Test
  public void seekTest() throws IOException {
    mInMemInputStream.seek(0);
    Assert.assertEquals(0, mInMemInputStream.getPos());
    IOException exception = null;
    try {
      mInMemInputStream.seek(-1);
    } catch (IOException e) {
      exception = e;
    }
    Assert.assertEquals("Seek position is negative: -1", exception.getMessage());
    try {
      mInMemInputStream.seek(FILE_LEN + 1);
    } catch (IOException e) {
      exception = e;
    }
    Assert.assertEquals("Seek position is past EOF: " + (FILE_LEN + 1) + ", fileSize = "
        + FILE_LEN, exception.getMessage());

    mUfsInputStream.seek(0);
    Assert.assertEquals(0, mUfsInputStream.getPos());
    try {
      mUfsInputStream.seek(-1);
    } catch (IOException e) {
      exception = e;
    }
    Assert.assertEquals("Seek position is negative: -1", exception.getMessage());
    try {
      mUfsInputStream.seek(FILE_LEN + 1);
    } catch (IOException e) {
      exception = e;
    }
    Assert.assertEquals("Seek position is past EOF: " + (FILE_LEN + 1) + ", fileSize = "
        + FILE_LEN, exception.getMessage());
  }
}
