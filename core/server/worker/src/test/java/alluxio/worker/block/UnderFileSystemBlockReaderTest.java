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

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsManager.UfsInfo;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;

import com.google.common.base.Suppliers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;

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
  private Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(new HashMap() {
    {
      put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, AlluxioTestDirectory
          .createTemporaryDirectory("UnderFileSystemBlockReaderTest").getAbsolutePath());
    }
  });

  @Before
  public void before() throws Exception {
    String ufsFolder = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String testFilePath = File.createTempFile("temp", null, new File(ufsFolder)).getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE * 2);
    BufferUtils.writeBufferToFile(testFilePath, buffer);

    mAlluxioBlockStore = Mockito.mock(BlockStore.class);
    mTempBlockMeta = Mockito.mock(TempBlockMeta.class);
    mUfsManager = Mockito.mock(UfsManager.class);
    UfsInfo ufsInfo = new UfsInfo(
        Suppliers.ofInstance(UnderFileSystem.Factory.create(testFilePath)),
        new AlluxioURI(testFilePath));
    Mockito.when(mAlluxioBlockStore
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.any(BlockStoreLocation.class),
            Mockito.anyLong()))
        .thenReturn(mTempBlockMeta);
    Mockito.when(mTempBlockMeta.getPath())
        .thenReturn(File.createTempFile("temp", null, new File(ufsFolder)).getAbsolutePath());
    Mockito.when(mUfsManager.get(Mockito.anyLong())).thenReturn(ufsInfo);

    mOpenUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder().setMaxUfsReadConcurrency(10)
        .setBlockSize(TEST_BLOCK_SIZE).setOffsetInFile(TEST_BLOCK_SIZE).setUfsPath(testFilePath)
        .build();
    mUnderFileSystemBlockMeta =
        new UnderFileSystemBlockMeta(SESSION_ID, BLOCK_ID, mOpenUfsBlockOptions);
  }

  @Test
  public void readFullBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);

    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE, buffer));

    mReader.close();
  }

  @Test
  public void readPartialBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE - 1);

    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE - 1, buffer));

    mReader.close();
  }

  @Test
  public void offset() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(2, TEST_BLOCK_SIZE - 2);

    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE + 2, (int) TEST_BLOCK_SIZE - 2, buffer));

    mReader.close();
  }

  @Test
  public void readOverlap() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 2, mAlluxioBlockStore, mUfsManager);
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
    mUnderFileSystemBlockMeta = new UnderFileSystemBlockMeta(SESSION_ID, BLOCK_ID,
        mOpenUfsBlockOptions.toBuilder().setNoCache(true).build());
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE, buffer));
    mReader.close();
  }

  @Test
  public void transferFullBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
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
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
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
