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
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Constants;
import tachyon.IntegrationTestConstants;
import tachyon.TachyonURI;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemCluster;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerContext;

/**
 * Integration tests for <code>tachyon.client.FileOutStream</code>. TODO(binfan): Run tests with
 * local writes enabled and disabled.
 */
public final class FileOutStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 32;
  private static final int BUFFER_BYTES = 100;
  private static final long WORKER_CAPACITY_BYTES = Constants.GB;
  private static final int QUOTA_UNIT_BYTES = 128;
  private static final int BLOCK_SIZE_BYTES = 128;
  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static OutStreamOptions sWriteBoth;
  private static OutStreamOptions sWriteTachyon;
  private static OutStreamOptions sWriteUnderStore;
  private static OutStreamOptions sWriteLocal;

  private TachyonFileSystem mTfs = null;
  private TachyonConf mMasterTachyonConf;

  @After
  public final void after() throws Exception {
    sLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    TachyonConf tachyonConf = WorkerContext.getConf();
    tachyonConf.set(Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES));
    // Only the Netty data server supports remote writes.
    tachyonConf.set(Constants.WORKER_DATA_SERVER, IntegrationTestConstants.NETTY_DATA_SERVER);
    sLocalTachyonCluster.start();
    mTfs = sLocalTachyonCluster.getClient();
    mMasterTachyonConf = sLocalTachyonCluster.getMasterTachyonConf();
    sWriteBoth = new OutStreamOptions.Builder(mMasterTachyonConf)
        .setTachyonStorageType(TachyonStorageType.STORE)
        .setUnderStorageType(UnderStorageType.SYNC_PERSIST).setBlockSize(BLOCK_SIZE_BYTES).build();
    sWriteTachyon = new OutStreamOptions.Builder(mMasterTachyonConf)
        .setTachyonStorageType(TachyonStorageType.STORE)
        .setUnderStorageType(UnderStorageType.NO_PERSIST).setBlockSize(BLOCK_SIZE_BYTES).build();
    sWriteUnderStore = new OutStreamOptions.Builder(mMasterTachyonConf)
        .setTachyonStorageType(TachyonStorageType.NO_STORE)
        .setUnderStorageType(UnderStorageType.SYNC_PERSIST).setBlockSize(BLOCK_SIZE_BYTES).build();
    sWriteLocal = new OutStreamOptions.Builder(mMasterTachyonConf)
        .setTachyonStorageType(TachyonStorageType.STORE)
        .setUnderStorageType(UnderStorageType.SYNC_PERSIST).setBlockSize(BLOCK_SIZE_BYTES)
        .setHostname(NetworkAddressUtils.getLocalHostName(ClientContext.getConf())).build();
  }

  @BeforeClass
  public static final void beforeClass() throws IOException {
    sLocalTachyonCluster =
        new LocalTachyonCluster(WORKER_CAPACITY_BYTES, QUOTA_UNIT_BYTES, BLOCK_SIZE_BYTES);
  }

  /**
   * Checks that we wrote the file correctly by reading it every possible way
   *
   * @param filePath path of the tmp file
   * @param underStorageType type of understorage write
   * @param fileLen length of the file
   * @param increasingByteArrayLen expected length of increasing bytes written in the file
   * @throws IOException
   */
  private void checkWrite(TachyonURI filePath, UnderStorageType underStorageType, int fileLen,
      int increasingByteArrayLen) throws IOException, TachyonException {
    for (OutStreamOptions op : getOptionSet()) {
      TachyonFile file = mTfs.open(filePath);
      FileInfo info = mTfs.getInfo(file);
      Assert.assertEquals(fileLen, info.getLength());
      InStreamOptions op2 = new InStreamOptions.Builder(mMasterTachyonConf)
          .setTachyonStorageType(op.getTachyonStorageType()).build();
      FileInStream is = mTfs.getInStream(file, op2);
      byte[] res = new byte[(int) info.getLength()];
      Assert.assertEquals((int) info.getLength(), is.read(res));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
      is.close();
    }

    if (underStorageType.isSyncPersist()) {
      TachyonFile file = mTfs.open(filePath);
      FileInfo info = mTfs.getInfo(file);
      String checkpointPath = info.getUfsPath();
      UnderFileSystem ufs = UnderFileSystem.get(checkpointPath, mMasterTachyonConf);

      InputStream is = ufs.open(checkpointPath);
      byte[] res = new byte[(int) info.getLength()];
      if (UnderFileSystemCluster.readEOFReturnsNegative() && 0 == res.length) {
        // Returns -1 for zero-sized byte array to indicate no more bytes available here.
        Assert.assertEquals(-1, is.read(res));
      } else {
        Assert.assertEquals((int) info.getLength(), is.read(res));
      }
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
      is.close();
    }
  }

  /**
   * Test <code>void write(int b)</code>.
   */
  @Test
  public void writeTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        writeTest1Util(new TachyonURI(uniqPath + "/file_" + k + "_" + op), k, op);
      }
    }
  }

  private void writeTest1Util(TachyonURI filePath, int len, OutStreamOptions op)
      throws IOException, TachyonException {
    FileOutStream os = mTfs.getOutStream(filePath, op);
    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();
    checkWrite(filePath, op.getUnderStorageType(), len, len);
  }

  /**
   * Test <code>void write(byte[] b)</code>.
   */
  @Test
  public void writeTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        writeTest2Util(new TachyonURI(uniqPath + "/file_" + k + "_" + op), k, op);
      }
    }
  }

  private void writeTest2Util(TachyonURI filePath, int len, OutStreamOptions op)
      throws IOException, TachyonException {
    FileOutStream os = mTfs.getOutStream(filePath, op);
    os.write(BufferUtils.getIncreasingByteArray(len));
    os.close();
    checkWrite(filePath, op.getUnderStorageType(), len, len);
  }

  /**
   * Test <code>void write(byte[] b, int off, int len)</code>.
   */
  @Test
  public void writeTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (OutStreamOptions op : getOptionSet()) {
        writeTest3Util(new TachyonURI(uniqPath + "/file_" + k + "_" + op), k, op);
      }
    }
  }

  private void writeTest3Util(TachyonURI filePath, int len, OutStreamOptions op)
      throws IOException, TachyonException {
    FileOutStream os = mTfs.getOutStream(filePath, op);
    os.write(BufferUtils.getIncreasingByteArray(0, len / 2), 0, len / 2);
    os.write(BufferUtils.getIncreasingByteArray(len / 2, len / 2), 0, len / 2);
    os.close();
    checkWrite(filePath, op.getUnderStorageType(), len, len / 2 * 2);
  }

  /**
   * Test writing to a file and specify the location to be localhost.
   *
   * @throws IOException if file can not be opened successfully.
   */
  @Test
  public void writeSpecifyLocalTest() throws IOException, TachyonException {
    TachyonURI filePath = new TachyonURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mTfs.getOutStream(filePath, sWriteLocal);
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();
    checkWrite(filePath, sWriteLocal.getUnderStorageType(), length, length);
  }

  /**
   * Test writing to a file for longer than HEARTBEAT_INTERVAL_MS to make sure the sessionId doesn't
   * change. Tracks [TACHYON-171].
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void longWriteChangesSessionId()
      throws IOException, InterruptedException, TachyonException {
    TachyonURI filePath = new TachyonURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mTfs.getOutStream(filePath, sWriteUnderStore);
    os.write((byte) 0);
    Thread.sleep(mMasterTachyonConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS) * 2);
    os.write((byte) 1);
    os.close();
    checkWrite(filePath, sWriteUnderStore.getUnderStorageType(), length, length);
  }

  /**
   * Tests if out-of-order writes are possible. Writes could be out-of-order when the following are
   * both true: - a "large" write (over half the internal buffer size) follows a smaller write. -
   * the "large" write does not cause the internal buffer to overflow.
   *
   * @throws IOException
   */
  @Test
  public void outOfOrderWriteTest() throws IOException, TachyonException {
    TachyonURI filePath = new TachyonURI(PathUtils.uniqPath());
    FileOutStream os = mTfs.getOutStream(filePath, sWriteTachyon);

    // Write something small, so it is written into the buffer, and not directly to the file.
    os.write((byte) 0);

    // A length greater than 0.5 * BUFFER_BYTES and less than BUFFER_BYTES.
    int length = (BUFFER_BYTES * 3) / 4;

    // Write a large amount of data (larger than BUFFER_BYTES/2, but will not overflow the buffer.
    os.write(BufferUtils.getIncreasingByteArray(1, length));
    os.close();

    checkWrite(filePath, sWriteTachyon.getUnderStorageType(), length + 1, length + 1);
  }

  private List<OutStreamOptions> getOptionSet() {
    List<OutStreamOptions> ret = new ArrayList<OutStreamOptions>(3);
    ret.add(sWriteBoth);
    ret.add(sWriteTachyon);
    ret.add(sWriteUnderStore);
    return ret;
  }
}
