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

package alluxio.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import alluxio.Constants;
import alluxio.IntegrationTestConstants;
import alluxio.LocalTachyonClusterResource;
import alluxio.TachyonURI;
import alluxio.client.block.RemoteBlockInStream;
import alluxio.client.block.TachyonBlockStore;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.conf.TachyonConf;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.TachyonException;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

/**
 * Integration tests for {@link alluxio.client.block.RemoteBlockInStream}.
 */
@RunWith(Parameterized.class)
public class RemoteBlockInStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource;
  private FileSystem mFileSystem = null;
  private String mDataServerClass;
  private String mNettyTransferType;
  private String mRemoteReaderClass;
  private TachyonConf mTachyonConf;
  private CreateFileOptions mWriteTachyon;
  private CreateFileOptions mWriteUnderStore;
  private OpenFileOptions mReadNoCache;
  private OpenFileOptions mReadCache;
  private int mWorkerToMasterHeartbeatIntervalMs;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // creates a new instance of RemoteBlockInStreamTest for each network type
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.MAPPED_TRANSFER, IntegrationTestConstants.NETTY_BLOCK_READER});
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.FILE_CHANNEL_TRANSFER,
        IntegrationTestConstants.NETTY_BLOCK_READER});
    // The transfer type is not applicable to the NIODataServer.
    list.add(new Object[] {IntegrationTestConstants.NIO_DATA_SERVER,
        IntegrationTestConstants.UNUSED_TRANSFER, IntegrationTestConstants.NETTY_BLOCK_READER});
    return list;
  }

  public RemoteBlockInStreamIntegrationTest(String dataServer, String transferType, String reader) {
    mDataServerClass = dataServer;
    mNettyTransferType = transferType;
    mRemoteReaderClass = reader;

    mLocalTachyonClusterResource = new LocalTachyonClusterResource(Constants.GB,
        Constants.GB, Constants.WORKER_DATA_SERVER, mDataServerClass,
        Constants.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, mNettyTransferType,
        Constants.USER_BLOCK_REMOTE_READER, mRemoteReaderClass,
        Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "100");
  }

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public final void before() throws Exception {
    mTachyonConf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
    mFileSystem = mLocalTachyonClusterResource.get().getClient();
    mWriteTachyon = StreamOptionUtils.getCreateFileOptionsMustCache(mTachyonConf);
    mWriteUnderStore = StreamOptionUtils.getCreateFileOptionsThrough(mTachyonConf);
    mReadCache = StreamOptionUtils.getOpenFileOptionsCache(mTachyonConf);
    mReadNoCache = StreamOptionUtils.getOpenFileOptionsNoCache(mTachyonConf);
    mWorkerToMasterHeartbeatIntervalMs =
        mTachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS);
  }

  /**
   * Test {@link RemoteBlockInStream#read()}. Read from underfs.
   */
  @Test
  public void readTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
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
        Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      } else {
        Assert.assertFalse(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      }

      is = mFileSystem.openFile(uri, mReadCache);
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
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);

      is = mFileSystem.openFile(uri, mReadCache);
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
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test {@link RemoteBlockInStream#read(byte[])}. Read from underfs.
   */
  @Test
  public void readTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
      byte[] ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      if (k == 0) {
        Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      } else {
        Assert.assertFalse(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      }

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test {@link RemoteBlockInStream#read(byte[], int, int)}. Read from underfs.
   */
  @Test
  public void readTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
      byte[] ret = new byte[k / 2];
      Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
      is.close();
      if (k == 0) {
        Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      } else {
        Assert.assertFalse(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      }

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret, 0, k));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test {@link RemoteBlockInStream#read()}. Read from remote data server.
   */
  @Test
  public void readTest4() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteTachyon, k);

      long blockId = mFileSystem.getStatus(uri).getBlockIds().get(0);
      BlockInfo info = TachyonBlockStore.get().getInfo(blockId);
      WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
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
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test {@link RemoteBlockInStream#read(byte[])}. Read from remote data server.
   */
  @Test
  public void readTest5() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteTachyon, k);

      long blockId = mFileSystem.getStatus(uri).getBlockIds().get(0);
      BlockInfo info = TachyonBlockStore.get().getInfo(blockId);
      WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
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
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test {@link RemoteBlockInStream#read(byte[], int, int)}. Read from remote data server.
   */
  @Test
  public void readTest6() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteTachyon, k);

      long blockId = mFileSystem.getStatus(uri).getBlockIds().get(0);
      BlockInfo info = TachyonBlockStore.get().getInfo(blockId);
      WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
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
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test {@link RemoteBlockInStream#read(byte[])}. Read from underfs.
   */
  @Test
  public void readTest7() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
      byte[] ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      Assert.assertEquals(-1, is.read(ret));
      is.close();
      Assert.assertFalse(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Test {@link RemoteBlockInStream#seek(long)}. Validate the expected exception for seeking a
   * negative position.
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
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
      try {
        is.seek(-1);
      } finally {
        is.close();
      }
    }
  }

  /**
   * Test {@link RemoteBlockInStream#seek(long)}. Validate the expected exception for seeking a
   * position that is past block size.
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
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
      try {
        is.seek(k + 1);
      } finally {
        is.close();
      }
    }
  }

  /**
   * Test {@link RemoteBlockInStream#seek(long)}.
   *
   * @throws IOException
   * @throws TachyonException
   */
  @Test
  public void seekTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);

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
   * Test {@link RemoteBlockInStream#skip(long)}.
   */
  @Test
  public void skipTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadCache);
      Assert.assertEquals(k / 2, is.skip(k / 2));
      Assert.assertEquals(k / 2, is.read());
      is.close();
      Assert.assertFalse(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);

      if (k >= 3) {
        is = mFileSystem.openFile(uri, mReadCache);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
        Assert.assertFalse(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
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
    TachyonURI uri = new TachyonURI(uniqPath);
    FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, len);

    FileInStream is = mFileSystem.openFile(uri, mReadCache);
    for (int i = 0; i < len; ++ i) {
      Assert.assertEquals(i, is.read());
    }
    is.close();
    Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
  }

  /**
   * Tests that not reading a file the whole way through then closing it will cause it to not
   * recache
   */
  @Test
  public void incompleteFileReadCancelsRecache() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonURI uri = new TachyonURI(uniqPath);
    FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, 2);

    FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
    Assert.assertEquals(0, is.read());
    is.close();
    Assert.assertFalse(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    is = mFileSystem.openFile(uri, mReadNoCache);
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
    TachyonURI uri = new TachyonURI(uniqPath);
    FileOutStream os = mFileSystem.createFile(uri, mWriteUnderStore);
    for (int i = 0; i < numBlocks; i ++) {
      for (int j = 0; j < blockSizeByte; j ++) {
        os.write((byte) (i * blockSizeByte + j));
      }
    }
    os.close();

    FileInStream is = mFileSystem.openFile(uri, mReadCache);
    for (int i = 0; i < blockSizeByte * numBlocks; i ++) {
      Assert.assertEquals((byte) i, is.read());
    }
    is.close();
    Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
  }

  /**
   * Tests that seeking around a file cached locally works.
   */
  @Test
  public void seekAroundLocalBlock() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    // The number of bytes per remote block read should be set to 100 in the before function
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath, 200, mWriteTachyon);
    FileInStream is = mFileSystem.openFile(new TachyonURI(uniqPath), mReadNoCache);
    Assert.assertEquals(0, is.read());
    is.seek(199);
    Assert.assertEquals(199, is.read());
    is.seek(99);
    Assert.assertEquals(99, is.read());
    is.close();
  }

  /**
   * Tests remote read stream lock in {@link RemoteBlockInStream}.
   */
  @Test
  public void remoteReadLockTest() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      TachyonURI uri = new TachyonURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteTachyon, k);

      long blockId = mFileSystem.getStatus(uri).getBlockIds().get(0);
      BlockInfo info = TachyonBlockStore.get().getInfo(blockId);

      WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
      InetSocketAddress workerInetAddr =
          new InetSocketAddress(workerAddr.getHost(), workerAddr.getDataPort());

      RemoteBlockInStream is =
          new RemoteBlockInStream(info.getBlockId(), info.getLength(), workerInetAddr);
      Assert.assertEquals(0, is.read());
      mFileSystem.delete(uri);

      // TODO(andrew): Consider using HeartbeatScheduler to make this more deterministic.
      CommonUtils.sleepMs(mWorkerToMasterHeartbeatIntervalMs * 2);

      // The file has been deleted.
      Assert.assertFalse(mFileSystem.exists(uri));
      // Look! We can still read the deleted file since we have a lock!
      byte[] ret = new byte[k / 2];
      Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
      is.close();
      Assert.assertFalse(mFileSystem.exists(uri));

      // Try to create an in stream again, and it should fail.
      RemoteBlockInStream is2 = null;
      try {
        is2 = new RemoteBlockInStream(info.getBlockId(), info.getLength(), workerInetAddr);
      } catch (IOException e) {
        Assert.assertTrue(e.getCause() instanceof BlockDoesNotExistException);
      } finally {
        if (is2 != null) {
          is2.close();
        }
      }
    }
  }
}
