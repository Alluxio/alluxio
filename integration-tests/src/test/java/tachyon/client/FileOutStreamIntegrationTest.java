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
import java.io.InputStream;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemCluster;

/**
 * Integration tests for <code>tachyon.client.FileOutStream</code>.
 */
public class FileOutStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 32;
  private static final int BUFFER_BYTES = 100;
  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static TachyonFS sTfs = null;

  private TachyonConf mMasterTachyonConf;

  @Before
  public final void before() throws IOException {
    mMasterTachyonConf = sLocalTachyonCluster.getMasterTachyonConf();
  }

  @AfterClass
  public static final void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
  }

  @BeforeClass
  public static final void beforeClass() throws IOException {
    sLocalTachyonCluster = new LocalTachyonCluster(10000, 128, 128);
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES));
    sLocalTachyonCluster.start(tachyonConf);
    sTfs = sLocalTachyonCluster.getClient();
  }

  /**
   * Checks that we wrote the file correctly by reading it every possible way
   *
   * @param filePath
   * @param op
   * @param fileLen
   * @throws IOException
   */
  private void checkWrite(TachyonURI filePath, WriteType op, int fileLen,
      int increasingByteArrayLen) throws IOException {
    for (ReadType rOp : ReadType.values()) {
      TachyonFile file = sTfs.getFile(filePath);
      InStream is = file.getInStream(rOp);
      Assert.assertEquals(fileLen, file.length());
      byte[] res = new byte[(int) file.length()];
      Assert.assertEquals((int) file.length(), is.read(res));
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
      is.close();
    }

    if (op.isThrough()) {
      TachyonFile file = sTfs.getFile(filePath);
      String checkpointPath = file.getUfsPath();
      UnderFileSystem ufs = UnderFileSystem.get(checkpointPath, mMasterTachyonConf);

      InputStream is = ufs.open(checkpointPath);
      byte[] res = new byte[(int) file.length()];
      if (UnderFileSystemCluster.readEOFReturnsNegative() && 0 == res.length) {
        // Returns -1 for zero-sized byte array to indicate no more bytes available here.
        Assert.assertEquals(-1, is.read(res));
      } else {
        Assert.assertEquals((int) file.length(), is.read(res));
      }
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
      is.close();
    }
  }

  /**
   * Test <code>void write(int b)</code>.
   */
  @Test
  public void writeTest1() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        writeTest1Util(new TachyonURI(uniqPath + "/file_" + k + "_" + op), op, k);
      }
    }
  }

  private void writeTest1Util(TachyonURI filePath, WriteType op, int len) throws IOException {
    int fileId = sTfs.createFile(filePath);
    TachyonFile file = sTfs.getFile(fileId);
    OutStream os = file.getOutStream(op);
    Assert.assertTrue(os instanceof FileOutStream);
    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();
    checkWrite(filePath, op, len, len);
  }

  /**
   * Test <code>void write(byte[] b)</code>.
   */
  @Test
  public void writeTest2() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        writeTest2Util(new TachyonURI(uniqPath + "/file_" + k + "_" + op), op, k);
      }
    }
  }

  private void writeTest2Util(TachyonURI filePath, WriteType op, int len) throws IOException {
    int fileId = sTfs.createFile(filePath);
    TachyonFile file = sTfs.getFile(fileId);
    OutStream os = file.getOutStream(op);
    Assert.assertTrue(os instanceof FileOutStream);
    os.write(TestUtils.getIncreasingByteArray(len));
    os.close();
    checkWrite(filePath, op, len, len);
  }

  /**
   * Test <code>void write(byte[] b, int off, int len)</code>.
   */
  @Test
  public void writeTest3() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        writeTest3Util(new TachyonURI(uniqPath + "/file_" + k + "_" + op), op, k);
      }
    }
  }

  private void writeTest3Util(TachyonURI filePath, WriteType op, int len) throws IOException {
    int fileId = sTfs.createFile(filePath);
    TachyonFile file = sTfs.getFile(fileId);
    OutStream os = file.getOutStream(op);
    Assert.assertTrue(os instanceof FileOutStream);
    os.write(TestUtils.getIncreasingByteArray(0, len / 2), 0, len / 2);
    os.write(TestUtils.getIncreasingByteArray(len / 2, len / 2), 0, len / 2);
    os.close();
    checkWrite(filePath, op, len, len / 2 * 2);
  }

  /**
   * Test writing to a file for longer than HEARTBEAT_INTERVAL_MS to make sure the userId doesn't
   * change. Tracks [TACHYON-171].
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void longWriteChangesUserId() throws IOException, InterruptedException {
    TachyonURI filePath = new TachyonURI(TestUtils.uniqPath());
    WriteType op = WriteType.THROUGH;
    int len = 2;
    int fileId = sTfs.createFile(filePath);
    long origId = sTfs.getUserId();
    TachyonFile file = sTfs.getFile(fileId);
    OutStream os = file.getOutStream(WriteType.THROUGH);
    Assert.assertTrue(os instanceof FileOutStream);
    os.write((byte) 0);
    Thread.sleep(mMasterTachyonConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS,
        Constants.SECOND_MS) * 2);
    Assert.assertEquals(origId, sTfs.getUserId());
    os.write((byte) 1);
    os.close();
    checkWrite(filePath, op, len, len);
  }

  /**
   * Tests if out-of-order writes are possible. Writes could be out-of-order when the following are
   * both true:
   * - a "large" write (over half the internal buffer size) follows a smaller write.
   * - the "large" write does not cause the internal buffer to overflow.
   * @throws IOException
   */
  @Test
  public void outOfOrderWriteTest() throws IOException {
    TachyonURI filePath = new TachyonURI(TestUtils.uniqPath());
    int fileId = sTfs.createFile(filePath);
    TachyonFile file = sTfs.getFile(fileId);
    OutStream os = file.getOutStream(WriteType.MUST_CACHE);

    // Write something small, so it is written into the buffer, and not directly to the file.
    os.write((byte) 0);

    // A length greater than 0.5 * BUFFER_BYTES and less than BUFFER_BYTES.
    int length = (BUFFER_BYTES * 3) / 4;

    // Write a large amount of data (larger than BUFFER_BYTES/2, but will not overflow the buffer.
    os.write(TestUtils.getIncreasingByteArray(1, length));
    os.close();

    checkWrite(filePath, WriteType.MUST_CACHE, length + 1, length + 1);
  }
}
