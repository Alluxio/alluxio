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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.thrift.LockBlockTOptions;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;
import alluxio.worker.block.options.OpenUfsBlockOptions;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TempBlockMeta.class})
public final class UnderFileSystemBlockReaderTest {
  private static final long TEST_BLOCK_SIZE = 1024;
  private static final long SESSION_ID = 1;
  private static final long BLOCK_ID = 2;

  private UnderFileSystemBlockReader mReader;
  private BlockStore mAlluxioBlockStore;
  private TempBlockMeta mTempBlockMeta;
  private UnderFileSystemBlockMeta mUnderFileSystemBlockMeta;
  private UfsManager mUfsManager;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mFolder.getRoot().getAbsolutePath());

    String testFilePath = mFolder.newFile().getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE * 2);
    BufferUtils.writeBufferToFile(testFilePath, buffer);

    mAlluxioBlockStore = Mockito.mock(BlockStore.class);
    mTempBlockMeta = Mockito.mock(TempBlockMeta.class);
    mUfsManager = Mockito.mock(UfsManager.class);
    Mockito.when(mAlluxioBlockStore
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.any(BlockStoreLocation.class),
            Mockito.anyLong())).thenReturn(mTempBlockMeta);
    Mockito.when(mTempBlockMeta.getPath()).thenReturn(mFolder.newFile().getAbsolutePath());
    Mockito.when(mUfsManager.get(Mockito.anyLong()))
        .thenReturn(UnderFileSystem.Factory.create(testFilePath));

    LockBlockTOptions options = new LockBlockTOptions();
    options.setMaxUfsReadConcurrency(10);
    options.setBlockSize(TEST_BLOCK_SIZE);
    options.setOffset(TEST_BLOCK_SIZE);
    options.setUfsPath(testFilePath);

    mUnderFileSystemBlockMeta =
        new UnderFileSystemBlockMeta(SESSION_ID, BLOCK_ID, new OpenUfsBlockOptions(options));
  }

  @After
  public void after() throws Exception {
    Configuration.defaultInit();
  }

  @Test
  public void readFullBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, false, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);

    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE, buffer));

    mReader.close();
  }

  @Test
  public void readPartialBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, false, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE - 1);

    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE - 1, buffer));

    mReader.close();
  }

  @Test
  public void offset() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, false, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(2, TEST_BLOCK_SIZE - 2);

    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE + 2, (int) TEST_BLOCK_SIZE - 2, buffer));

    mReader.close();
  }

  @Test
  public void readOverlap() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 2, false, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(2, TEST_BLOCK_SIZE - 2);
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE + 2, (int) TEST_BLOCK_SIZE - 2, buffer));

    buffer = mReader.read(0, TEST_BLOCK_SIZE - 2);
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE - 2, buffer));

    buffer = mReader.read(3, TEST_BLOCK_SIZE);
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE + 3, (int) TEST_BLOCK_SIZE - 3, buffer));

    mReader.close();
  }

  @Test
  public void noCache() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, true, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE, buffer));
    mReader.close();
  }

  @Test
  public void transferFullBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, false, mAlluxioBlockStore, mUfsManager);
    ByteBuf buf =
        PooledByteBufAllocator.DEFAULT.buffer((int) TEST_BLOCK_SIZE * 2, (int) TEST_BLOCK_SIZE * 2);
    try {
      while (buf.writableBytes() > 0 && mReader.transferTo(buf) != -1) {
      }
      Assert.assertTrue(BufferUtils
          .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE,
              buf.nioBuffer()));
      mReader.close();
    } finally {
      buf.release();
    }
  }

  @Test
  public void transferPartialBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, false, mAlluxioBlockStore, mUfsManager);
    ByteBuf buf =
        PooledByteBufAllocator.DEFAULT.buffer((int) TEST_BLOCK_SIZE / 2, (int) TEST_BLOCK_SIZE / 2);
    try {
      while (buf.writableBytes() > 0 && mReader.transferTo(buf) != -1) {
      }
      Assert.assertTrue(BufferUtils
          .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE / 2,
              buf.nioBuffer()));
      mReader.close();
    } finally {
      buf.release();
    }
  }
}
