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

package alluxio.worker.block;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsManager.UfsClient;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;

public final class UnderFileSystemBlockReaderTest {
  private static final long TEST_BLOCK_SIZE = 1024;
  private static final long SESSION_ID = 1;
  private static final long BLOCK_ID = 2;

  private UnderFileSystemBlockReader mReader;
  private BlockStore mAlluxioBlockStore;
  private UnderFileSystemBlockMeta mUnderFileSystemBlockMeta;
  private UfsManager.UfsClient mUfsClient;
  private UfsInputStreamCache mUfsInstreamCache;
  private Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private Counter mUfsBytesRead;
  private Meter mUfsBytesReadThroughput;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new HashMap<PropertyKey, Object>() {
        {
          put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, AlluxioTestDirectory
              .createTemporaryDirectory("UnderFileSystemBlockReaderTest-RootUfs")
              .getAbsolutePath());
          // ensure tiered storage uses different tmp dir for each test case
          put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, AlluxioTestDirectory
              .createTemporaryDirectory("UnderFileSystemBlockReaderTest-WorkerDataFolder")
              .getAbsolutePath());
          put(PropertyKey.WORKER_TIERED_STORE_LEVELS, 1);
        }
      }, ServerConfiguration.global());

  @Before
  public void before() throws Exception {
    String ufsFolder = ServerConfiguration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String testFilePath = File.createTempFile("temp", null, new File(ufsFolder)).getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE * 2);
    BufferUtils.writeBufferToFile(testFilePath, buffer);

    mAlluxioBlockStore = new TieredBlockStore();
    mUfsInstreamCache = new UfsInputStreamCache();
    mUfsClient = new UfsClient(
        () -> UnderFileSystem.Factory.create(testFilePath,
            UnderFileSystemConfiguration.defaults(ServerConfiguration.global())),
        new AlluxioURI(testFilePath));

    mOpenUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder().setMaxUfsReadConcurrency(10)
        .setBlockSize(TEST_BLOCK_SIZE).setOffsetInFile(TEST_BLOCK_SIZE).setUfsPath(testFilePath)
        .build();
    mUnderFileSystemBlockMeta =
        new UnderFileSystemBlockMeta(SESSION_ID, BLOCK_ID, mOpenUfsBlockOptions);
    mUfsBytesRead = MetricsSystem.counterWithTags(
        MetricKey.WORKER_BYTES_READ_UFS.getName(),
        MetricKey.WORKER_BYTES_READ_UFS.isClusterAggregated(),
        MetricInfo.TAG_UFS, MetricsSystem.escape(mUfsClient.getUfsMountPointUri()));
    mUfsBytesReadThroughput = MetricsSystem.meterWithTags(
        MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.getName(),
        MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.isClusterAggregated(),
        MetricInfo.TAG_UFS,
        MetricsSystem.escape(mUfsClient.getUfsMountPointUri()));
  }

  private void checkTempBlock(long start, long length) throws Exception {
    Assert.assertTrue(mAlluxioBlockStore.hasTempBlockMeta(BLOCK_ID));
    mAlluxioBlockStore.commitBlock(SESSION_ID, BLOCK_ID, false);
    long lockId = mAlluxioBlockStore.lockBlock(SESSION_ID, BLOCK_ID);
    BlockReader reader = mAlluxioBlockStore.getBlockReader(SESSION_ID, BLOCK_ID, lockId);
    Assert.assertEquals(length, reader.getLength());
    ByteBuffer buffer = reader.read(0, length);
    assertTrue(BufferUtils.equalIncreasingByteBuffer((int) start, (int) length, buffer));
    reader.close();
  }

  @Test
  public void readFullBlock() throws Exception {
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 0, false,
        mAlluxioBlockStore, mUfsClient, mUfsInstreamCache, mUfsBytesRead, mUfsBytesReadThroughput);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
    mReader.close();
    checkTempBlock(0, TEST_BLOCK_SIZE);
  }

  @Test
  public void readPartialBlock() throws Exception {
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 0, false,
        mAlluxioBlockStore, mUfsClient, mUfsInstreamCache, mUfsBytesRead, mUfsBytesReadThroughput);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE - 1);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE - 1, buffer));
    mReader.close();
    // partial block should not be cached
    Assert.assertFalse(mAlluxioBlockStore.hasTempBlockMeta(BLOCK_ID));
  }

  @Test
  public void offset() throws Exception {
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 0, false,
        mAlluxioBlockStore, mUfsClient, mUfsInstreamCache, mUfsBytesRead, mUfsBytesReadThroughput);
    ByteBuffer buffer = mReader.read(2, TEST_BLOCK_SIZE - 2);
    assertTrue(BufferUtils
        .equalIncreasingByteBuffer(2, (int) TEST_BLOCK_SIZE - 2, buffer));
    mReader.close();
    // partial block should not be cached
    Assert.assertFalse(mAlluxioBlockStore.hasTempBlockMeta(BLOCK_ID));
  }

  @Test
  public void readOverlap() throws Exception {
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 2, false,
        mAlluxioBlockStore, mUfsClient, mUfsInstreamCache, mUfsBytesRead, mUfsBytesReadThroughput);
    ByteBuffer buffer = mReader.read(2, TEST_BLOCK_SIZE - 2);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(2, (int) TEST_BLOCK_SIZE - 2, buffer));
    buffer = mReader.read(0, TEST_BLOCK_SIZE - 2);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE - 2, buffer));
    buffer = mReader.read(3, TEST_BLOCK_SIZE);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(3, (int) TEST_BLOCK_SIZE - 3, buffer));
    mReader.close();
    // block should be cached as two reads covers the full block
    checkTempBlock(0, TEST_BLOCK_SIZE);
  }

  @Test
  public void readFullBlockNoCache() throws Exception {
    mUnderFileSystemBlockMeta = new UnderFileSystemBlockMeta(SESSION_ID, BLOCK_ID,
        mOpenUfsBlockOptions.toBuilder().setNoCache(true).build());
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 0, false,
        mAlluxioBlockStore, mUfsClient, mUfsInstreamCache, mUfsBytesRead, mUfsBytesReadThroughput);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);
    // read should succeed even if error is thrown when caching
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
    mReader.close();
    Assert.assertFalse(mAlluxioBlockStore.hasTempBlockMeta(BLOCK_ID));
  }

  @Test
  public void readFullBlockRequestSpaceError() throws Exception {
    BlockStore errorThrowingBlockStore = spy(mAlluxioBlockStore);
    doThrow(new WorkerOutOfSpaceException("Ignored"))
        .when(errorThrowingBlockStore)
        .requestSpace(anyLong(), anyLong(), anyLong());
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 0, false,
        errorThrowingBlockStore, mUfsClient, mUfsInstreamCache,
        mUfsBytesRead, mUfsBytesReadThroughput);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
    mReader.close();
    Assert.assertFalse(mAlluxioBlockStore.hasTempBlockMeta(BLOCK_ID));
  }

  @Test
  public void readFullBlockRequestCreateBlockError() throws Exception {
    BlockStore errorThrowingBlockStore = spy(mAlluxioBlockStore);
    doThrow(new WorkerOutOfSpaceException("Ignored")).when(errorThrowingBlockStore)
        .createBlock(anyLong(), anyLong(), any(AllocateOptions.class));
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 0, false,
        errorThrowingBlockStore, mUfsClient, mUfsInstreamCache,
        mUfsBytesRead, mUfsBytesReadThroughput);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
    mReader.close();
    Assert.assertFalse(mAlluxioBlockStore.hasTempBlockMeta(BLOCK_ID));
  }

  @Test
  public void transferFullBlock() throws Exception {
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 0, false,
        mAlluxioBlockStore, mUfsClient, mUfsInstreamCache, mUfsBytesRead, mUfsBytesReadThroughput);
    ByteBuf buf =
        PooledByteBufAllocator.DEFAULT.buffer((int) TEST_BLOCK_SIZE * 2, (int) TEST_BLOCK_SIZE * 2);
    try {
      while (buf.writableBytes() > 0 && mReader.transferTo(buf) != -1) {
      }
      assertTrue(BufferUtils
          .equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buf.nioBuffer()));
      mReader.close();
    } finally {
      buf.release();
    }
    checkTempBlock(0, TEST_BLOCK_SIZE);
  }

  @Test
  public void transferPartialBlock() throws Exception {
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 0, false,
        mAlluxioBlockStore, mUfsClient, mUfsInstreamCache, mUfsBytesRead, mUfsBytesReadThroughput);
    ByteBuf buf =
        PooledByteBufAllocator.DEFAULT.buffer((int) TEST_BLOCK_SIZE / 2, (int) TEST_BLOCK_SIZE / 2);
    try {
      while (buf.writableBytes() > 0 && mReader.transferTo(buf) != -1) {
      }
      assertTrue(BufferUtils
          .equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE / 2, buf.nioBuffer()));
      mReader.close();
    } finally {
      buf.release();
    }
    // partial block should not be cached
    Assert.assertFalse(mAlluxioBlockStore.hasTempBlockMeta(BLOCK_ID));
  }

  @Test
  public void getLocation() throws Exception {
    mReader = UnderFileSystemBlockReader.create(mUnderFileSystemBlockMeta, 0, false,
        mAlluxioBlockStore, mUfsClient, mUfsInstreamCache, mUfsBytesRead, mUfsBytesReadThroughput);
    assertTrue(mReader.getLocation().startsWith(mOpenUfsBlockOptions.getUfsPath()));
  }
}
