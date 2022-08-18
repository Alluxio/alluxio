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

package alluxio.worker.page;

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.NoopUfsManager;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.UfsInputStreamCache;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

@RunWith(Parameterized.class)
public class PagedBlockReaderTest {
  private static final long CARDINALITY_BYTE = 256;
  // this test uses local file system to create test blocks, so one block is one file
  // therefore offset in file is always 0
  private static final long OFFSET_IN_FILE = 0;
  private static final long MOUNT_ID = 100;
  // use fixed block size and varying page and buffer size
  private static final long BLOCK_SIZE = Constants.MB;
  private static final int MAX_UFS_READ_CONCURRENCY = 1;
  private static final long BLOCK_ID = 0L;
  @Rule
  public TemporaryFolder mTempFolderRule = new TemporaryFolder();
  @Rule
  public ConfigurationRule mConfRule =
      new ConfigurationRule(ImmutableMap.of(), Configuration.modifiableGlobal());

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        /* page size, buffer size */
        {Constants.KB, Constants.KB},
        {Constants.KB, 200},
        {Constants.KB, 201},
        {Constants.MB, Constants.KB},
        {Constants.MB, 200},
        {Constants.MB - 1, Constants.KB},
        {2 * Constants.MB, Constants.KB},
    });
  }

  @Parameterized.Parameter
  public long mPageSize;

  @Parameterized.Parameter(1)
  public int mBufferSize;

  private PagedBlockReader mReader;

  @Before
  public void setup() throws Exception {
    mConfRule.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(mPageSize));
    File tempFolder = mTempFolderRule.newFolder();
    Path blockFilePath = tempFolder.toPath().resolve("block");
    createTempUfsBlock(blockFilePath, BLOCK_SIZE);

    NoopUfsManager ufsManager = new NoopUfsManager();
    ufsManager.addMount(MOUNT_ID, new AlluxioURI(tempFolder.getAbsolutePath()),
        new UnderFileSystemConfiguration(new InstancedConfiguration(new AlluxioProperties()),
            true));
    List<PagedBlockStoreDir> pagedBlockStoreDirs = PagedBlockStoreDir.fromPageStoreDirs(
        PageStoreDir.createPageStoreDirs(Configuration.global()));
    mReader = new PagedBlockReader(
        new ByteArrayCacheManager(),
        ufsManager,
        new UfsInputStreamCache(),
        Configuration.global(),
        BLOCK_ID,
        createUfsBlockOptions(blockFilePath.toAbsolutePath().toString())
    );
  }

  @Test
  public void sequentialReadAtOnce() throws Exception {
    try (PagedBlockReader reader = mReader) {
      ByteBuffer buffer = reader.read(0, BLOCK_SIZE);
      assertTrue(BufferUtils.equalIncreasingByteBuffer(0, buffer.remaining(), buffer));
    }
  }

  @Test
  public void sequentialReadMultipleRounds() throws Exception {
    final int bytesToReadPerIter = mBufferSize;
    long pos = 0;
    try (PagedBlockReader reader = mReader;
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      while (pos < BLOCK_SIZE) {
        int length = (int) Math.min(bytesToReadPerIter, BLOCK_SIZE - pos);
        ByteBuffer buffer = reader.read(pos, length);
        pos += buffer.remaining();
        baos.write(buffer.array());
      }
      assertTrue(BufferUtils.equalIncreasingByteArray((byte) 0, baos.size(), baos.toByteArray()));
    }
  }

  @Test
  public void randomReadToEnd() throws Exception {
    final int bytesToReadPerIter = mBufferSize;
    final long initialPos = new Random().nextInt((int) BLOCK_SIZE);
    long pos = initialPos;
    try (PagedBlockReader reader = mReader;
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      while (pos < BLOCK_SIZE) {
        int length = (int) Math.min(bytesToReadPerIter, BLOCK_SIZE - pos);
        ByteBuffer buffer = reader.read(pos, length);
        pos += buffer.remaining();
        baos.write(buffer.array());
      }
      assertTrue(BufferUtils.equalIncreasingByteArray(
          (byte) (initialPos % CARDINALITY_BYTE), baos.size(), baos.toByteArray()));
    }
  }

  @Test
  public void randomReadJumpAround() throws Exception {
    final int bytesToReadPerIter = mBufferSize;
    final Random rng = new Random();
    try (PagedBlockReader reader = mReader) {
      for (int i = 0; i < 100; i++) { // try 100 random initial positions
        final long pos = rng.nextInt((int) BLOCK_SIZE);
        final int length = (int) Math.min(bytesToReadPerIter, BLOCK_SIZE - pos);
        ByteBuffer buffer = reader.read(pos, length);

        assertTrue(BufferUtils.equalIncreasingByteBuffer(
            (byte) (pos % CARDINALITY_BYTE), buffer.remaining(), buffer));
      }
    }
  }

  private static Protocol.OpenUfsBlockOptions createUfsBlockOptions(String ufsPath) {
    return Protocol.OpenUfsBlockOptions.newBuilder()
        .setMountId(MOUNT_ID)
        .setBlockSize(BLOCK_SIZE)
        .setOffsetInFile(OFFSET_IN_FILE)
        .setMaxUfsReadConcurrency(MAX_UFS_READ_CONCURRENCY)
        .setUfsPath(ufsPath)
        .build();
  }

  private static void createTempUfsBlock(Path destPath, long blockSize) throws Exception {
    try (OutputStream os =
             Files.newOutputStream(destPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      long bytesWritten = 0;
      final int bufSize = Constants.KB; // must be a multiple of cardinality(byte)
      byte[] buf = BufferUtils.getIncreasingByteArray(0, bufSize);
      while (bytesWritten < blockSize) {
        if (bytesWritten + bufSize > blockSize) {
          byte[] bytesLeft = new byte[(int) (blockSize - bytesWritten)];
          System.arraycopy(buf, 0, bytesLeft, 0, bytesLeft.length);
          os.write(bytesLeft);
          break;
        }
        os.write(buf);
        bytesWritten += bufSize;
      }
    }
  }
}
