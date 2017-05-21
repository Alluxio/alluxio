/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.BaseIntegrationTest;
import alluxio.IntegrationTestConstants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for reading from a remote worker.
 */
@RunWith(Parameterized.class)
public class RemoteReadIntegrationTest extends BaseIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource;
  private FileSystem mFileSystem = null;
  private CreateFileOptions mWriteAlluxio;
  private CreateFileOptions mWriteUnderStore;
  private OpenFileOptions mReadNoCache;
  private OpenFileOptions mReadCache;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // creates a new instance of RemoteBlockInStreamTest for each network type
    List<Object[]> list = new ArrayList<>();
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.MAPPED_TRANSFER});
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.FILE_CHANNEL_TRANSFER});
    return list;
  }

  /**
   * Constructor for {@link RemoteReadIntegrationTest}.
   *
   * @param dataServer the address of the worker's data server
   * @param transferType the file transfer type used by the worker
   */
  public RemoteReadIntegrationTest(String dataServer, String transferType) {
    mLocalAlluxioClusterResource = new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.WORKER_DATA_SERVER_CLASS, dataServer)
        .setProperty(PropertyKey.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, transferType)
        .setProperty(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "100")
        .setProperty(PropertyKey.USER_UFS_BLOCK_READ_CONCURRENCY_MAX, 2)
        .build();
  }

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteAlluxio = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
    mWriteUnderStore = CreateFileOptions.defaults().setWriteType(WriteType.THROUGH);
    mReadCache = OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE);
    mReadNoCache = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
  }

  /**
   * Tests the single byte read API from a remote location when the data is only in the
   * underlying storage.
   */
  @Test
  public void readTest1() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
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
      if (k == 0) {
        Assert.assertEquals(100, mFileSystem.getStatus(uri).getInMemoryPercentage());
      } else {
        Assert.assertNotEquals(100, mFileSystem.getStatus(uri).getInMemoryPercentage());
      }

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      value = is.read();
      cnt = 0;
      while (value != -1) {
        Assert.assertTrue(value >= 0);
        Assert.assertTrue(value < 256);
        ret[cnt++] = (byte) value;
        value = is.read();
      }
      Assert.assertEquals(cnt, k);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertEquals(100, mFileSystem.getStatus(uri).getInMemoryPercentage());

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      value = is.read();
      cnt = 0;
      while (value != -1) {
        Assert.assertTrue(value >= 0);
        Assert.assertTrue(value < 256);
        ret[cnt++] = (byte) value;
        value = is.read();
      }
      Assert.assertEquals(cnt, k);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertEquals(100, mFileSystem.getStatus(uri).getInMemoryPercentage());
    }
  }

  /**
   * Tests the batch read API from a remote location when the data is only in the underlying
   * storage.
   */
  @Test
  public void readTest2() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
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
   * Tests the batch read API with offset and length from a remote location when the data is only in
   * the underlying storage.
   */
  @Test
  public void readTest3() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
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
   * Tests the single byte read API from a remote location when the data is in an Alluxio worker.
   */
  @Test
  public void readTest4() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteAlluxio, k);

      long blockId = mFileSystem.getStatus(uri).getBlockIds().get(0);
      AlluxioBlockStore blockStore = AlluxioBlockStore.create();
      BlockInfo info = blockStore.getInfo(blockId);
      WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
      BlockInStream is = BlockInStream
          .create(FileSystemContext.INSTANCE, info.getBlockId(), info.getLength(), workerAddr, null,
              InStreamOptions.defaults());
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
      Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    }
  }

  /**
   * Tests the batch read API from a remote location when the data is only in an Alluxio worker.
   */
  @Test
  public void readTest5() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteAlluxio, k);

      long blockId = mFileSystem.getStatus(uri).getBlockIds().get(0);
      BlockInfo info = AlluxioBlockStore.create().getInfo(blockId);
      WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
      BlockInStream is = BlockInStream
          .create(FileSystemContext.INSTANCE, info.getBlockId(), info.getLength(), workerAddr, null,
              InStreamOptions.defaults());
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
   * Tests the batch read API with offset and length from a remote location when the data is in an
   * Alluxio worker.
   */
  @Test
  public void readTest6() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteAlluxio, k);

      long blockId = mFileSystem.getStatus(uri).getBlockIds().get(0);
      BlockInfo info = AlluxioBlockStore.create().getInfo(blockId);
      WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
      BlockInStream is = BlockInStream
          .create(FileSystemContext.INSTANCE, info.getBlockId(), info.getLength(), workerAddr, null,
              InStreamOptions.defaults());
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
   * Tests the batch read API from a remote location when the data is only in the underlying
   * storage.
   */
  @Test
  public void readTest7() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
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
   * Tests the read API from a remote location after a delay longer than the netty heartbeat
   * timeout.
   */
  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS, "1sec"})
  public void heartbeat1() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    int size = 100;
    AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + size);
    FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, size);

    FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
    CommonUtils.sleepMs(2000);
    byte[] ret = new byte[size];
    Assert.assertEquals(size, is.read(ret));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(size, ret));
    Assert.assertEquals(-1, is.read(ret));
    is.close();
    Assert.assertFalse(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
  }

  /**
   * Validates the expected exception for seeking a negative position.
   */
  @Test
  public void seekExceptionTest1() throws Exception {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), -1));
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      try (FileInStream is = mFileSystem.openFile(uri, mReadNoCache)) {
        is.seek(-1);
      }
    }
  }

  /**
   * Validates the expected exception for seeking a position that is past block size.
   */
  @Test
  public void seekExceptionTest2() throws Exception {
    mThrown.expect(IllegalArgumentException.class);
    mThrown
        .expectMessage(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), 1));
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      try (FileInStream is = mFileSystem.openFile(uri, mReadNoCache)) {
        is.seek(k + 1);
      }
    }
  }

  /**
   * Tests seeking through data which is in a remote location.
   */
  @Test
  public void seek() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
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
   * Tests skipping through data which is in a remote location.
   */
  @Test
  public void skip() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadCache);
      Assert.assertEquals(k / 2, is.skip(k / 2));
      Assert.assertEquals(k / 2, is.read());
      is.close();
      Assert.assertEquals(100, mFileSystem.getStatus(uri).getInMemoryPercentage());

      if (k >= 3) {
        is = mFileSystem.openFile(uri, mReadCache);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
        Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Tests that reading a file the whole way through with the STORE ReadType will recache it.
   */
  @Test
  public void completeFileReadTriggersRecache() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    int len = 2;
    AlluxioURI uri = new AlluxioURI(uniqPath);
    FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, len);

    FileInStream is = mFileSystem.openFile(uri, mReadCache);
    for (int i = 0; i < len; ++i) {
      Assert.assertEquals(i, is.read());
    }
    is.close();
    Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
  }

  /**
   * Tests that not reading a file the whole way through then closing it will cause it to not
   * recache.
   */
  @Test
  public void incompleteFileReadCancelsRecache() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    AlluxioURI uri = new AlluxioURI(uniqPath);
    FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, 2);

    FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
    Assert.assertEquals(0, is.read());
    is.close();
    Assert.assertFalse(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
    is = mFileSystem.openFile(uri, mReadNoCache);
    is.close();
  }

  /**
   * Tests that reading a file consisting of more than one block from the underfs works.
   */
  @Test
  public void readMultiBlockFile() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    int blockSizeByte = 10;
    int numBlocks = 10;
    AlluxioURI uri = new AlluxioURI(uniqPath);
    FileOutStream os = mFileSystem.createFile(uri, mWriteUnderStore);
    for (int i = 0; i < numBlocks; i++) {
      for (int j = 0; j < blockSizeByte; j++) {
        os.write((byte) (i * blockSizeByte + j));
      }
    }
    os.close();

    FileInStream is = mFileSystem.openFile(uri, mReadCache);
    for (int i = 0; i < blockSizeByte * numBlocks; i++) {
      Assert.assertEquals((byte) i, is.read());
    }
    is.close();
    Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
  }

  /**
   * Tests that seeking around a file cached locally works.
   */
  @Test
  public void seekAroundLocalBlock() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    // The number of bytes per remote block read should be set to 100 in the before function
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath, 200, mWriteAlluxio);
    FileInStream is = mFileSystem.openFile(new AlluxioURI(uniqPath), mReadNoCache);
    Assert.assertEquals(0, is.read());
    is.seek(199);
    Assert.assertEquals(199, is.read());
    is.seek(99);
    Assert.assertEquals(99, is.read());
    is.close();
  }

  /**
   * Tests remote reads lock blocks correctly.
   */
  @Test
  public void remoteReadLock() throws Exception {
    HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10, TimeUnit.SECONDS);

    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteAlluxio, k);
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

      long blockId = mFileSystem.getStatus(uri).getBlockIds().get(0);
      BlockInfo info = AlluxioBlockStore.create().getInfo(blockId);

      WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
      BlockInStream is = BlockInStream
          .create(FileSystemContext.INSTANCE, info.getBlockId(), info.getLength(), workerAddr, null,
              InStreamOptions.defaults());
      Assert.assertEquals(0, is.read());
      mFileSystem.delete(uri);
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

      // The file has been deleted.
      Assert.assertFalse(mFileSystem.exists(uri));
      // Look! We can still read the deleted file since we have a lock!
      byte[] ret = new byte[k / 2];
      Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
      is.close();
      Assert.assertFalse(mFileSystem.exists(uri));

      // Try to create an in stream again, and it should fail.
      BlockInStream is2 = null;
      try {
        is2 = BlockInStream
            .create(FileSystemContext.INSTANCE, info.getBlockId(), info.getLength(), workerAddr,
                null, InStreamOptions.defaults());
      } catch (NotFoundException e) {
        // Expected since the file has been deleted.
      } finally {
        if (is2 != null) {
          is2.close();
        }
      }
    }
  }
}
