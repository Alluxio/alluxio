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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import tachyon.Constants;
import tachyon.IntegrationTestConstants;
import tachyon.TachyonURI;
import tachyon.client.block.RemoteBlockInStream;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerContext;

/**
 * Integration tests for <code>tachyon.client.RemoteBlockInStream</code>.
 */
@RunWith(Parameterized.class)
public class RemoteBlockInStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTfs = null;
  private String mDataServerClass;
  private String mNettyTransferType;
  private String mRemoteReaderClass;
  private TachyonConf mTachyonConf;
  private OutStreamOptions mWriteNative;
  private OutStreamOptions mWriteUnderStore;
  private InStreamOptions mReadNoCache;
  private InStreamOptions mReadCache;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // creates a new instance of RemoteBlockInStreamTest for each network type
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.MAPPED_TRANSFER, IntegrationTestConstants.TCP_BLOCK_READER});
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.MAPPED_TRANSFER, IntegrationTestConstants.NETTY_BLOCK_READER});
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.FILE_CHANNEL_TRANSFER, IntegrationTestConstants.TCP_BLOCK_READER});
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.FILE_CHANNEL_TRANSFER,
        IntegrationTestConstants.NETTY_BLOCK_READER});
    // The transfer type is not applicable to the NIODataServer.
    list.add(new Object[] {IntegrationTestConstants.NIO_DATA_SERVER,
        IntegrationTestConstants.UNUSED_TRANSFER, IntegrationTestConstants.TCP_BLOCK_READER});
    list.add(new Object[] {IntegrationTestConstants.NIO_DATA_SERVER,
        IntegrationTestConstants.UNUSED_TRANSFER, IntegrationTestConstants.NETTY_BLOCK_READER});
    return list;
  }

  public RemoteBlockInStreamIntegrationTest(String dataServer, String transferType, String reader) {
    mDataServerClass = dataServer;
    mNettyTransferType = transferType;
    mRemoteReaderClass = reader;
  }

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    TachyonConf conf = WorkerContext.getConf();
    conf.set(Constants.WORKER_DATA_SERVER, mDataServerClass);
    conf.set(Constants.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, mNettyTransferType);
    conf.set(Constants.USER_BLOCK_REMOTE_READER, mRemoteReaderClass);
    conf.set(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "100");

    mLocalTachyonCluster = new LocalTachyonCluster(Constants.GB, Constants.KB, Constants.GB);
    mLocalTachyonCluster.start();

    mTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mTfs = mLocalTachyonCluster.getClient();
    mWriteNative =
        new OutStreamOptions.Builder(mTachyonConf).setNativeStorageType(NativeStorageType.STORE)
            .setUnderStorageType(UnderStorageType.NO_PERSIST).build();
    mWriteUnderStore =
        new OutStreamOptions.Builder(mTachyonConf)
            .setNativeStorageType(NativeStorageType.NO_STORE)
            .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
    mReadCache =
        new InStreamOptions.Builder(mTachyonConf).setNativeStorageType(NativeStorageType.STORE)
            .build();
    mReadNoCache =
        new InStreamOptions.Builder(mTachyonConf)
            .setNativeStorageType(NativeStorageType.NO_STORE).build();
  }

  /**
   * Test <code>void read()</code>. Read from underfs.
   */
  @Test
  public void readTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteUnderStore);

      FileInStream is = mTfs.getInStream(f, mReadNoCache);
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
      if (k == 0) {
        Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
      } else {
        Assert.assertFalse(mTfs.getInfo(f).getInMemoryPercentage() == 100);
      }

      is = mTfs.getInStream(f, mReadCache);
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
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);

      is = mTfs.getInStream(f, mReadCache);
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
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test <code>void read(byte[] b)</code>. Read from underfs.
   */
  @Test
  public void readTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteUnderStore);

      FileInStream is = mTfs.getInStream(f, mReadNoCache);
      byte[] ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      if (k == 0) {
        Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
      } else {
        Assert.assertFalse(mTfs.getInfo(f).getInMemoryPercentage() == 100);
      }

      is = mTfs.getInStream(f, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);

      is = mTfs.getInStream(f, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>. Read from underfs.
   */
  @Test
  public void readTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteUnderStore);

      FileInStream is = mTfs.getInStream(f, mReadNoCache);
      byte[] ret = new byte[k / 2];
      Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
      is.close();
      if (k == 0) {
        Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
      } else {
        Assert.assertFalse(mTfs.getInfo(f).getInMemoryPercentage() == 100);
      }

      is = mTfs.getInStream(f, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret, 0, k));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);

      is = mTfs.getInStream(f, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test <code>void read()</code>. Read from remote data server.
   */
  @Test
  public void readTest4() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteNative);

      long blockId = mTfs.getInfo(f).getBlockIds().get(0);
      BlockInfo info = TachyonBlockStore.get().getInfo(blockId);
      NetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
      RemoteBlockInStream is =
          new RemoteBlockInStream(info.getBlockId(), info.getLength(), new InetSocketAddress(
              workerAddr.getHost(), workerAddr.getDataPort()));
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
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test <code>void read(byte[] b)</code>. Read from remote data server.
   */
  @Test
  public void readTest5() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteNative);

      long blockId = mTfs.getInfo(f).getBlockIds().get(0);
      BlockInfo info = TachyonBlockStore.get().getInfo(blockId);
      NetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
      RemoteBlockInStream is =
          new RemoteBlockInStream(info.getBlockId(), info.getLength(), new InetSocketAddress(
              workerAddr.getHost(), workerAddr.getDataPort()));
      byte[] ret = new byte[k];
      int start = 0;
      while (start < k) {
        int read = is.read(ret);
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(start, read, ret));
        start += read;
      }
      is.close();
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>. Read from remote data server.
   */
  @Test
  public void readTest6() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteNative);

      long blockId = mTfs.getInfo(f).getBlockIds().get(0);
      BlockInfo info = TachyonBlockStore.get().getInfo(blockId);
      NetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
      RemoteBlockInStream is =
          new RemoteBlockInStream(info.getBlockId(), info.getLength(), new InetSocketAddress(
              workerAddr.getHost(), workerAddr.getDataPort()));
      byte[] ret = new byte[k / 2];
      int start = 0;
      while (start < k / 2) {
        int read = is.read(ret, 0, (k / 2) - start);
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(start, read, ret));
        start += read;
      }
      is.close();
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test <code>void read(byte[] b)</code>. Read from underfs.
   */
  @Test
  public void readTest7() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteUnderStore);

      FileInStream is = mTfs.getInStream(f, mReadNoCache);
      byte[] ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      Assert.assertEquals(-1, is.read(ret));
      is.close();
      Assert.assertFalse(mTfs.getInfo(f).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test <code>void seek(long pos)</code>. Validate the expected exception for seeking a negative
   * position.
   *
   * @throws IOException
   * @throws TachyonException
   */
  @Test
  public void seekExceptionTest1() throws IOException, TachyonException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("Seek position is negative: -1");
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteUnderStore);

      FileInStream is = mTfs.getInStream(f, mReadNoCache);
      try {
        is.seek(-1);
      } finally {
        is.close();
      }
    }
  }

  /**
   * Test <code>void seek(long pos)</code>. Validate the expected exception for seeking a position
   * that is past block size.
   *
   * @throws IOException
   * @throws TachyonException
   */
  @Test
  public void seekExceptionTest2() throws IOException, TachyonException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("Seek position past end of file: 1");
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteUnderStore);

      FileInStream is = mTfs.getInStream(f, mReadNoCache);
      try {
        is.seek(k + 1);
      } finally {
        is.close();
      }
    }
  }

  /**
   * Test <code>void seek(long pos)</code>.
   *
   * @throws IOException
   * @throws TachyonException
   */
  @Test
  public void seekTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteUnderStore);

      FileInStream is = mTfs.getInStream(f, mReadNoCache);

      Assert.assertEquals(0, is.read());
      is.seek(k / 3);
      Assert.assertEquals(k / 3, is.read());
      is.seek(k / 2);
      Assert.assertEquals(k / 2, is.read());
      is.seek(k / 4);
      Assert.assertEquals(k / 4, is.read());
      is.close();
    }
  }

  /**
   * Test <code>long skip(long len)</code>.
   */
  @Test
  public void skipTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonFile f =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k, k, mWriteUnderStore);

      FileInStream is = mTfs.getInStream(f, mReadCache);
      Assert.assertEquals(k / 2, is.skip(k / 2));
      Assert.assertEquals(k / 2, is.read());
      is.close();
      Assert.assertFalse(mTfs.getInfo(f).getInMemoryPercentage() == 100);

      if (k >= 3) {
        is = mTfs.getInStream(f, mReadCache);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
        Assert.assertFalse(mTfs.getInfo(f).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Tests that reading a file the whole way through with the STORE ReadType will recache it
   */
  @Test
  public void completeFileReadTriggersRecache() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    int len = 2;
    TachyonFile f =
        TachyonFSTestUtils.createByteFile(mTfs, uniqPath, len, mWriteUnderStore);

    FileInStream is = mTfs.getInStream(f, mReadCache);
    for (int i = 0; i < len; ++ i) {
      Assert.assertEquals(i, is.read());
    }
    is.close();
    Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
  }

  /**
   * Tests that not reading a file the whole way through then closing it will cause it to not
   * recache
   */
  @Test
  public void incompleteFileReadCancelsRecache() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonFile f =
        TachyonFSTestUtils.createByteFile(mTfs, uniqPath, 2, mWriteUnderStore);

    FileInStream is = mTfs.getInStream(f, mReadNoCache);
    Assert.assertEquals(0, is.read());
    is.close();
    Assert.assertFalse(mTfs.getInfo(f).getInMemoryPercentage() == 100);
    is = mTfs.getInStream(f, mReadNoCache);
    is.close();
  }

  /**
   * Tests that reading a file consisting of more than one block from the underfs works
   */
  @Test
  public void readMultiBlockFile() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    int blockSizeByte = 10;
    int numBlocks = 10;
    FileOutStream os = mTfs.getOutStream(new TachyonURI(uniqPath), mWriteUnderStore);
    for (int i = 0; i < numBlocks; i ++) {
      for (int j = 0; j < blockSizeByte; j ++) {
        os.write((byte) (i * blockSizeByte + j));
      }
    }
    os.close();

    TachyonFile f = mTfs.open(new TachyonURI(uniqPath));
    FileInStream is = mTfs.getInStream(f, mReadCache);
    for (int i = 0; i < blockSizeByte * numBlocks; i ++) {
      Assert.assertEquals((byte) i, is.read());
    }
    is.close();
    Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
  }

  /**
   * Tests that seeking around a file cached locally works.
   */
  @Test
  public void seekAroundLocalBlock() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    // The number of bytes per remote block read should be set to 100 in the before function
    TachyonFile f = TachyonFSTestUtils.createByteFile(mTfs, uniqPath, 200, mWriteNative);
    FileInStream is = mTfs.getInStream(f, mReadNoCache);
    Assert.assertEquals(0, is.read());
    is.seek(199);
    Assert.assertEquals(199, is.read());
    is.seek(99);
    Assert.assertEquals(99, is.read());
    is.close();
  }
}
