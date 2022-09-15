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

import static org.junit.Assert.assertEquals;
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
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.UfsInputStreamCache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
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

  @Parameterized.Parameters(name = "PageSize: {0}, BufferSize: {1}, Offset: {2}")
  public static Collection<Object[]> data() {
    Integer[][] params = new Integer[][]{
        /* page size, buffer size */
        {Constants.KB, Constants.KB},
        {Constants.KB, 200},
        {Constants.KB, 201},
        {Constants.MB, Constants.KB},
        {Constants.MB, 200},
        {Constants.MB - 1, Constants.KB},
        {2 * Constants.MB, Constants.KB},
    };
    List<Integer> offsets = ImmutableList.of(0, 1, (int) (BLOCK_SIZE - 1), (int) BLOCK_SIZE);

    ImmutableList.Builder<Object[]> listBuilder = ImmutableList.builder();
    // cartesian product
    for (Integer[] paramPair : params) {
      for (int offset : offsets) {
        listBuilder.add(new Integer[] {paramPair[0], paramPair[1], offset});
      }
    }
    return listBuilder.build();
  }

  @Parameterized.Parameter
  public long mPageSize;

  @Parameterized.Parameter(1)
  public int mBufferSize;

  @Parameterized.Parameter(2)
  public int mOffset;

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
        new PagedBlockMeta(BLOCK_ID, BLOCK_SIZE, pagedBlockStoreDirs.get(0)),
        mOffset,
        Optional.of(createUfsBlockOptions(blockFilePath.toAbsolutePath().toString()))
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

  @Test
  public void sequentialTransferAllAtOnce() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate((int) (BLOCK_SIZE - mOffset));
    buffer.mark();
    ByteBuf wrapped = Unpooled.wrappedBuffer(buffer);
    wrapped.clear();
    if (BLOCK_SIZE == mOffset) {
      assertEquals(-1, mReader.transferTo(wrapped));
    } else {
      assertEquals(BLOCK_SIZE - mOffset, mReader.transferTo(wrapped));
    }
    assertTrue(BufferUtils.equalIncreasingByteBuffer(
        (byte) (mOffset % CARDINALITY_BYTE), buffer.remaining(), buffer));
  }

  @Test
  public void sequentialTransferMultipleTimes() throws Exception {
    final int bytesToReadPerIter = mBufferSize;
    final int totalReadable = (int) (BLOCK_SIZE - mOffset);
    ByteBuffer buffer = ByteBuffer.allocate(totalReadable);

    int bytesRead = 0;
    while (bytesRead < totalReadable) {
      int bytesToRead = Math.min(bytesToReadPerIter, totalReadable - bytesRead);
      ByteBuf buf = Unpooled.buffer(bytesToRead, bytesToRead);
      final int actualRead = mReader.transferTo(buf);
      if (actualRead == -1) {
        break;
      }
      assertEquals(bytesToRead, actualRead);
      bytesRead += actualRead;
      buffer.put(buf.nioBuffer());
    }
    buffer.flip();
    assertTrue(BufferUtils.equalIncreasingByteBuffer(
        (byte) (mOffset % CARDINALITY_BYTE), buffer.remaining(), buffer));
  }

  private static UfsBlockReadOptions createUfsBlockOptions(String ufsPath) {
    return new UfsBlockReadOptions(MOUNT_ID, OFFSET_IN_FILE, ufsPath);
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
