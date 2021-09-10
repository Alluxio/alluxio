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

package alluxio.client.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.block.stream.TestBlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.util.ClientTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.resource.CloseableResource;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Tests for the {@link AlluxioFileInStream} class.
 *
 * It is a parameterized test that checks different caching behaviors when the blocks are located at
 * different locations.
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest({AlluxioBlockStore.class})
public final class AlluxioFileInStreamTest {
  private static final long BLOCK_LENGTH = 100L;
  private final BlockInStreamSource mBlockSource;
  private final long mFileSize;
  private final long mNumBlocks;
  private AlluxioBlockStore mBlockStore;
  private FileSystemContext mContext;
  private FileInfo mInfo;
  private URIStatus mStatus;

  private final InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  private List<TestBlockInStream> mInStreams;

  private AlluxioFileInStream mTestStream;
  private MockedStatic mMockedStaticBlockStore;

  /**
   * @return a list of all sources of where the blocks reside and file size
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      {BlockInStreamSource.PROCESS_LOCAL, 350L},
      {BlockInStreamSource.NODE_LOCAL, 350L},
      {BlockInStreamSource.UFS, 350L},
      {BlockInStreamSource.REMOTE, 350L},
      {BlockInStreamSource.REMOTE, 800L}
    });
  }

  /**
   * @param blockSource the source of the block to read
   * @param fileSize file size in bytes
   */
  public AlluxioFileInStreamTest(BlockInStreamSource blockSource, long fileSize) {
    mBlockSource = blockSource;
    mFileSize = fileSize;
    mNumBlocks = (mFileSize - 1) / BLOCK_LENGTH + 1;
  }

  private long getBlockLength(int blockIndex) {
    return Math.min(mFileSize, BLOCK_LENGTH * (blockIndex + 1)) - BLOCK_LENGTH * blockIndex;
  }

  /**
   * Sets up the context and streams before a test runs.
   */
  @Before
  public void before() throws Exception {
    mInfo = new FileInfo().setBlockSizeBytes(BLOCK_LENGTH).setLength(mFileSize);

    ClientTestUtils.setSmallBufferSizes(mConf);
    mConf.set(PropertyKey.USER_BLOCK_READ_RETRY_SLEEP_MIN, "1ms");
    mConf.set(PropertyKey.USER_BLOCK_READ_RETRY_SLEEP_MAX, "5ms");
    mConf.set(PropertyKey.USER_BLOCK_READ_RETRY_MAX_DURATION, "1s");

    BlockWorkerClient client = mock(BlockWorkerClient.class);
    doNothing().when(client).cache(any());

    mContext = mock(FileSystemContext.class);
    when(mContext.getClientContext()).thenReturn(ClientContext.create(mConf));
    when(mContext.getClusterConf()).thenReturn(mConf);
    when(mContext.getPathConf(any(AlluxioURI.class))).thenReturn(mConf);
    when(mContext.getNodeLocalWorker()).thenReturn(new WorkerNetAddress());
    when(mContext.getCachedWorkers()).thenReturn(new ArrayList<>());
    when(mContext.acquireBlockWorkerClient(any()))
        .thenReturn(new CloseableResource<BlockWorkerClient>(client) {
          @Override
          public void close() {}
        });
    mBlockStore = mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mContext)).thenReturn(mBlockStore);

    // Set up BufferedBlockInStreams and caching streams
    mInStreams = new ArrayList<>();
    List<Long> blockIds = new ArrayList<>();
    List<FileBlockInfo> fileBlockInfos = new ArrayList<>();
    for (int i = 0; i < mNumBlocks; i++) {
      blockIds.add((long) i);
      FileBlockInfo fbInfo = new FileBlockInfo().setBlockInfo(new BlockInfo().setBlockId(i));
      fileBlockInfos.add(fbInfo);
      final byte[] input = BufferUtils
          .getIncreasingByteArray((int) (i * BLOCK_LENGTH), (int) getBlockLength(i));
      mInStreams.add(new TestBlockInStream(input, i, input.length, false, mBlockSource));
      when(mContext.getCachedWorkers())
          .thenReturn(Arrays.asList(new BlockWorkerInfo(new WorkerNetAddress(), 0, 0)));
      when(mBlockStore.getInStream(eq((long) i), any(InStreamOptions.class), any()))
          .thenAnswer(invocation -> {
            long blockId = (Long) invocation.getArguments()[0];
            return mInStreams.get((int) blockId).isClosed() ? new TestBlockInStream(input,
                blockId, input.length, false, mBlockSource) : mInStreams.get((int) blockId);
          });
      when(mBlockStore.getInStream(eq(new BlockInfo().setBlockId(i)), any(InStreamOptions.class),
          any())).thenAnswer(invocation -> {
            long blockId = ((BlockInfo) invocation.getArguments()[0]).getBlockId();
            return mInStreams.get((int) blockId).isClosed() ? new TestBlockInStream(input,
                blockId, input.length, false, mBlockSource) : mInStreams.get((int) blockId);
          });
    }
    mInfo.setBlockIds(blockIds);
    mInfo.setFileBlockInfos(fileBlockInfos).setReplicationMax(1);
    mStatus = new URIStatus(mInfo);

    OpenFilePOptions readOptions =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream = new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, readOptions,
        mConf), mContext);
  }

  @After
  public void after() throws Exception {
    mTestStream.close();
    ClientTestUtils.resetClient(mConf);
  }

  /**
   * Tests that reading through the file one byte at a time will yield the correct data.
   */
  @Test
  public void singleByteRead() throws Exception {
    for (int i = 0; i < mFileSize; i++) {
      assertEquals(i & 0xff, mTestStream.read());
    }
  }

  /**
   * Tests that reading half of a file works.
   */
  @Test
  public void readHalfFile() throws Exception {
    testReadBuffer((int) (mFileSize / 2));
  }

  /**
   * Tests that reading a part of a file works.
   */
  @Test
  public void readPartialBlock() throws Exception {
    testReadBuffer((int) (BLOCK_LENGTH / 2));
  }

  /**
   * Tests that reading the complete block works.
   */
  @Test
  public void readBlock() throws Exception {
    testReadBuffer((int) BLOCK_LENGTH);
  }

  /**
   * Tests that reading the complete block works and the BlockInStream is closed.
   */
  @Test
  public void readBlockStreamCloseOnEnd() throws Exception {
    int dataRead = (int) BLOCK_LENGTH;
    byte[] buffer = new byte[dataRead];
    mTestStream.read(buffer);
    assertEquals(true, mInStreams.get(0).isClosed());
    assertArrayEquals(BufferUtils.getIncreasingByteArray(dataRead), buffer);
  }

  /**
   * Tests that reading the complete file works.
   */
  @Test
  public void readFile() throws Exception {
    testReadBuffer((int) mFileSize);
  }

  /**
   * Tests that reading the complete file works and all streams are closed when to the end of file.
   */
  @Test
  public void readFileStreamCloseOnEnd() throws Exception {
    int dataRead = (int) mFileSize;
    byte[] buffer = new byte[dataRead];
    mTestStream.read(buffer);

    for (int i = 0; i < mNumBlocks; i++) {
      assertEquals(true, mInStreams.get(i).isClosed());
    }
    assertArrayEquals(BufferUtils.getIncreasingByteArray(dataRead), buffer);
  }

  /**
   * Tests that reading a buffer at an offset writes the bytes to the correct places.
   */
  @Test
  public void readOffset() throws IOException {
    int offset = (int) (BLOCK_LENGTH / 3);
    int len = (int) BLOCK_LENGTH;
    byte[] buffer = new byte[offset + len];
    // Create expectedBuffer containing `offset` 0's followed by `len` increasing bytes
    byte[] expectedBuffer = new byte[offset + len];
    System.arraycopy(BufferUtils.getIncreasingByteArray(len), 0, expectedBuffer, offset, len);
    mTestStream.read(buffer, offset, len);
    assertArrayEquals(expectedBuffer, buffer);
  }

  /**
   * Read through the file in small chunks and verify each chunk.
   */
  @Test
  public void readManyChunks() throws IOException {
    int chunksize = 10;
    // chunksize must divide FILE_LENGTH evenly for this test to work
    assertEquals(0, mFileSize % chunksize);
    byte[] buffer = new byte[chunksize];
    int offset = 0;
    for (int i = 0; i < mFileSize / chunksize; i++) {
      mTestStream.read(buffer, 0, chunksize);
      assertArrayEquals(BufferUtils.getIncreasingByteArray(offset, chunksize), buffer);
      offset += chunksize;
    }
  }

  /**
   * Tests that {@link FileInStream#remaining()} is correctly updated during reads, skips, and
   * seeks.
   */
  @Test
  public void testRemaining() throws IOException {
    assumeTrue(mFileSize > 310);
    assertEquals(mFileSize, mTestStream.remaining());
    mTestStream.read();
    assertEquals(mFileSize - 1, mTestStream.remaining());
    mTestStream.read(new byte[150]);
    assertEquals(mFileSize - 151, mTestStream.remaining());
    mTestStream.skip(140);
    assertEquals(mFileSize - 291, mTestStream.remaining());
    mTestStream.seek(310);
    assertEquals(mFileSize - 310, mTestStream.remaining());
    mTestStream.seek(130);
    assertEquals(mFileSize - 130, mTestStream.remaining());
  }

  /**
   * Tests seek, particularly that seeking over part of a block will cause us not to cache it, and
   * cancels the existing cache stream.
   */
  @Test
  public void testSeek() throws IOException {
    assumeTrue(mFileSize >= BLOCK_LENGTH * 3.1);
    int seekAmount = (int) (BLOCK_LENGTH / 2);
    int readAmount = (int) (BLOCK_LENGTH * 2);
    byte[] buffer = new byte[readAmount];
    // Seek halfway into block 1
    mTestStream.seek(seekAmount);
    // Read two blocks from 0.5 to 2.5
    mTestStream.read(buffer);
    assertArrayEquals(BufferUtils.getIncreasingByteArray(seekAmount, readAmount), buffer);

    // second block is cached if the block is not local
    byte[] expected = mBlockSource != BlockInStreamSource.REMOTE ? new byte[0]
        : BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH);

    // Seek to current position (does nothing)
    mTestStream.seek(seekAmount + readAmount);
    // Seek a short way past start of block 3
    mTestStream.seek((long) (BLOCK_LENGTH * 3.1));
    assertEquals(BufferUtils.byteToInt((byte) (BLOCK_LENGTH * 3.1)), mTestStream.read());
    mTestStream.seek(mFileSize);
  }

  /**
   * Tests seeking back to the beginning of a block after the block's remaining is 0.
   */
  @Test
  public void seekToBeginningAfterReadingWholeBlock() throws IOException {
    // Read the whole block.
    int blockSize = (int) BLOCK_LENGTH;
    byte[] block = new byte[blockSize];
    mTestStream.read(block);
    assertArrayEquals(BufferUtils.getIncreasingByteArray(0, blockSize), block);

    // Seek to the beginning of the current block, then read half of it.
    mTestStream.seek(0);
    int halfBlockSize = blockSize / 2;
    byte[] halfBlock = new byte[halfBlockSize];
    mTestStream.read(halfBlock);
    assertArrayEquals(BufferUtils.getIncreasingByteArray(0, halfBlockSize), halfBlock);
  }

  /**
   * Tests seeking to the beginning of the last block after reaching EOF.
   */
  @Test
  public void seekToLastBlockAfterReachingEOF() throws IOException {
    mTestStream.read(new byte[(int) mFileSize]);
    mTestStream.seek(mFileSize - BLOCK_LENGTH);
    byte[] block = new byte[(int) BLOCK_LENGTH];
    mTestStream.read(block);
    assertArrayEquals(BufferUtils.getIncreasingByteArray(
        (int) (mFileSize - BLOCK_LENGTH), (int) BLOCK_LENGTH), block);
  }

  /**
   * Tests seeking to EOF, then seeking to position 0 and read the whole file.
   */
  @Test
  public void seekToEOFBeforeReadingFirstBlock() throws IOException {
    mTestStream.seek(mFileSize);
    mTestStream.seek(0);
    byte[] block = new byte[(int) BLOCK_LENGTH];
    mTestStream.read(block);
    assertArrayEquals(
        BufferUtils.getIncreasingByteArray(0, (int) BLOCK_LENGTH), block);
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks backward for more than a block.
   */
  @Test
  public void longSeekBackwardCachingPartiallyReadBlocks() throws IOException {
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream =
        new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, options, mConf), mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 4 + BLOCK_LENGTH);
    int readAmount = (int) (BLOCK_LENGTH * 3 - BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount - seekAmount);

    // Block 2 is cached though it is not fully read.
    validatePartialCaching(2, (int) BLOCK_LENGTH / 2);
  }

  /**
   * Tests reading and seeking with no local worker. Nothing should be cached.
   */
  @Test
  public void testSeekWithNoLocalWorker() throws IOException {
    // Overrides the get local worker call
    when(mContext.getNodeLocalWorker()).thenReturn(null);
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream =
        new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, options, mConf), mContext);
    int readAmount = (int) (BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    // read and seek several times
    mTestStream.read(buffer);
    assertEquals(readAmount, mInStreams.get(0).getBytesRead());
    mTestStream.seek(BLOCK_LENGTH + BLOCK_LENGTH / 2);
    mTestStream.seek(0);

    // only reads the read amount, regardless of block source
    assertEquals(readAmount, mInStreams.get(0).getBytesRead());
    assertEquals(0, mInStreams.get(1).getBytesRead());
  }

  @Test
  public void seekAndClose() throws IOException {
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream =
        new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, options, mConf), mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 2);
    mTestStream.seek(seekAmount);
    mTestStream.close();

    // Block 0 is cached though it is not fully read.
    validatePartialCaching(0, 0);
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks backward within 1 block.
   */
  @Test
  public void shortSeekBackwardCachingPartiallyReadBlocks() throws IOException {
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream =
        new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, options, mConf), mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 4);
    int readAmount = (int) (BLOCK_LENGTH * 2 - BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount - seekAmount);

    // Block 1 is cached though it is not fully read.
    validatePartialCaching(1, (int) BLOCK_LENGTH / 2);

    // Seek many times. It will cache block 1 only once.
    for (int i = 0; i <= seekAmount; i++) {
      mTestStream.seek(readAmount - seekAmount - i);
    }
    validatePartialCaching(1, (int) BLOCK_LENGTH / 2);
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks forward for more than a block.
   */
  @Test
  public void longSeekForwardCachingPartiallyReadBlocks() throws IOException {
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream = new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, options, mConf),
        mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 4 + BLOCK_LENGTH);
    int readAmount = (int) (BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount + seekAmount);

    // Block 0 is cached though it is not fully read.
    validatePartialCaching(0, readAmount);

    // Block 1 is being cached though its prefix it not read.
    validatePartialCaching(1, 0);
    mTestStream.close();
    validatePartialCaching(1, 0);
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks forward within a block.
   */
  @Test
  public void shortSeekForwardCachingPartiallyReadBlocks() throws IOException {
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream =
        new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, options, mConf), mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 4);
    int readAmount = (int) (BLOCK_LENGTH * 2 - BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount + seekAmount);

    // Block 1 (till seek pos) is being cached.
    validatePartialCaching(1, (int) BLOCK_LENGTH / 2);

    // Seek forward many times. The prefix is always cached.
    for (int i = 0; i < seekAmount; i++) {
      mTestStream.seek(readAmount + seekAmount + i);
      validatePartialCaching(1, (int) BLOCK_LENGTH / 2);
    }
  }

  /**
   * Tests skipping backwards when the seek buffer size is smaller than block size.
   */
  @Test
  public void seekBackwardSmallSeekBuffer() throws IOException {
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream =
        new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, options, mConf), mContext);
    int readAmount = (int) (BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    mTestStream.seek(readAmount - 1);

    validatePartialCaching(0, readAmount);
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks forward for more than a block
   * and then seek to the file beginning.
   */
  @Test
  public void seekBackwardToFileBeginning() throws IOException {
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream =
        new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, options, mConf), mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 4 + BLOCK_LENGTH);

    // Seek forward.
    mTestStream.seek(seekAmount);

    // Block 1 is partially cached though it is not fully read.
    validatePartialCaching(1, 0);

    // Seek backward.
    mTestStream.seek(0);

    // Block 1 is fully cached though it is not fully read.
    validatePartialCaching(1, 0);

    mTestStream.close();

    // block 0 is cached
    validatePartialCaching(0, 0);
  }

  /**
   * Tests skip, particularly that skipping the start of a block will cause us not to cache it, and
   * cancels the existing cache stream.
   */
  @Test
  public void testSkip() throws IOException {
    assumeTrue(mNumBlocks > 3);
    int skipAmount = (int) (BLOCK_LENGTH / 2);
    int readAmount = (int) (BLOCK_LENGTH * 2);
    byte[] buffer = new byte[readAmount];
    // Skip halfway into block 1
    mTestStream.skip(skipAmount);
    // Read two blocks from 0.5 to 2.5
    mTestStream.read(buffer);
    assertArrayEquals(BufferUtils.getIncreasingByteArray(skipAmount, readAmount), buffer);

    assertEquals(0, mTestStream.skip(0));
    // Skip the next half block, bringing us to block 3
    assertEquals(BLOCK_LENGTH / 2, mTestStream.skip(BLOCK_LENGTH / 2));
    assertEquals(BufferUtils.byteToInt((byte) (BLOCK_LENGTH * 3)), mTestStream.read());
  }

  /**
   * Tests that {@link IOException}s thrown by the {@link AlluxioBlockStore} are properly
   * propagated.
   */
  @Test
  public void failGetInStream() throws IOException {
    when(mBlockStore.getInStream(any(BlockInfo.class), any(InStreamOptions.class), any()))
        .thenThrow(new UnavailableException("test exception"));
    try {
      mTestStream.read();
      fail("block store should throw exception");
    } catch (IOException e) {
      assertEquals("test exception", e.getMessage());
    }
  }

  /**
   * Tests that reading out of bounds properly returns -1.
   */
  @Test
  public void readOutOfBounds() throws IOException {
    mTestStream.read(new byte[(int) mFileSize]);
    assertEquals(-1, mTestStream.read());
    assertEquals(-1, mTestStream.read(new byte[10]));
  }

  /**
   * Tests that specifying an invalid offset/length for a buffer read throws the right exception.
   */
  @Test
  public void readBadBuffer() throws IOException {
    try {
      mTestStream.read(new byte[10], 5, 6);
      fail("the buffer read of invalid offset/length should fail");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(PreconditionMessage.ERR_BUFFER_STATE.toString(), 10, 5, 6),
          e.getMessage());
    }
  }

  /**
   * Tests that seeking to a negative position will throw the right exception.
   */
  @Test
  public void seekNegative() throws IOException {
    try {
      mTestStream.seek(-1);
      fail("seeking negative position should fail");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), -1),
          e.getMessage());
    }
  }

  /**
   * Tests that seeking past the end of the stream will throw the right exception.
   */
  @Test
  public void seekPastEnd() throws IOException {
    try {
      mTestStream.seek(mFileSize + 1);
      fail("seeking past the end of the stream should fail");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(),
          mFileSize + 1), e.getMessage());
    }
  }

  /**
   * Tests that skipping a negative amount correctly reports that 0 bytes were skipped.
   */
  @Test
  public void skipNegative() throws IOException {
    assertEquals(0, mTestStream.skip(-10));
  }

  @Test
  public void positionedRead() throws IOException {
    byte[] b = new byte[(int) BLOCK_LENGTH];
    mTestStream.positionedRead(BLOCK_LENGTH, b, 0, b.length);
    assertArrayEquals(BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int)
        BLOCK_LENGTH), b);
  }

  /**
   * Tests the BlockInStream is closed when reading to the end of the block.
   */
  @Test
  public void positionedReadStreamCloseOnEnd() throws IOException {
    byte[] b = new byte[(int) BLOCK_LENGTH];
    mTestStream.positionedRead(0, b, 0, b.length);
    assertEquals(true, mInStreams.get(0).isClosed());
    assertArrayEquals(BufferUtils.getIncreasingByteArray((int) 0, (int)
        BLOCK_LENGTH), b);
  }

  @Test
  public void multiBlockPositionedRead() throws IOException {
    byte[] b = new byte[(int) BLOCK_LENGTH * 2];
    mTestStream.positionedRead(BLOCK_LENGTH / 2, b, 0, b.length);
    assertArrayEquals(BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH / 2, (int)
        BLOCK_LENGTH * 2), b);
  }

  @Test
  public void readOneRetry() throws Exception {
    long offset = 37;
    // Setups a broken stream for the first block to throw an exception.
    TestBlockInStream workingStream = mInStreams.get(0);
    TestBlockInStream brokenStream = mock(TestBlockInStream.class);
    when(mBlockStore
        .getInStream(any(BlockInfo.class), any(InStreamOptions.class), any()))
        .thenReturn(brokenStream).thenReturn(workingStream);
    when(brokenStream.read()).thenThrow(new UnavailableException("test exception"));
    when(brokenStream.getPos()).thenReturn(offset);

    mTestStream.seek(offset);
    int b = mTestStream.read();

    doReturn(0).when(brokenStream).read();
    verify(brokenStream, times(1)).read();
    assertEquals(offset, b);
  }

  @Test
  public void readBufferRetry() throws Exception {
    TestBlockInStream workingStream = mInStreams.get(0);
    TestBlockInStream brokenStream = mock(TestBlockInStream.class);
    when(mBlockStore
        .getInStream(any(BlockInfo.class), any(InStreamOptions.class), any()))
        .thenReturn(brokenStream).thenReturn(workingStream);
    when(brokenStream.read(any(ByteBuffer.class), anyInt(), anyInt()))
        .thenThrow(new UnavailableException("test exception"));
    when(brokenStream.getPos()).thenReturn(BLOCK_LENGTH / 2);

    mTestStream.seek(BLOCK_LENGTH / 2);
    byte[] b = new byte[(int) BLOCK_LENGTH * 2];
    mTestStream.read(b, 0, b.length);

    doReturn(0).when(brokenStream).read(any(ByteBuffer.class), anyInt(), anyInt());
    verify(brokenStream, times(1))
        .read(any(ByteBuffer.class), anyInt(), anyInt());
    assertArrayEquals(BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH / 2, (int)
        BLOCK_LENGTH * 2), b);
  }

  @Test
  public void positionedReadRetry() throws Exception {
    TestBlockInStream workingStream = mInStreams.get(0);
    TestBlockInStream brokenStream = mock(TestBlockInStream.class);
    when(mBlockStore
        .getInStream(eq(0L), any(InStreamOptions.class), any()))
        .thenReturn(brokenStream).thenReturn(workingStream);
    when(brokenStream.positionedRead(anyLong(), any(byte[].class), anyInt(), anyInt()))
        .thenThrow(new UnavailableException("test exception"));

    byte[] b = new byte[(int) BLOCK_LENGTH * 2];
    mTestStream.positionedRead(BLOCK_LENGTH / 2, b, 0, b.length);

    doReturn(0)
        .when(brokenStream).positionedRead(anyLong(), any(byte[].class), anyInt(), anyInt());
    verify(brokenStream, times(1))
        .positionedRead(anyLong(), any(byte[].class), anyInt(), anyInt());
    assertArrayEquals(BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH / 2, (int)
        BLOCK_LENGTH * 2), b);
  }

  /**
   * Tests that when the underlying blocks are inconsistent with the metadata in terms of block
   * length, an exception is thrown rather than client hanging indefinitely. This case may happen if
   * the file in Alluxio and UFS is out of sync.
   */
  @Test
  public void blockInStreamOutOfSync() throws Exception {
    when(mBlockStore.getInStream(any(BlockInfo.class), any(InStreamOptions.class), any()))
        .thenAnswer(new Answer<BlockInStream>() {
          @Override
          public BlockInStream answer(InvocationOnMock invocation) throws Throwable {
            return new TestBlockInStream(new byte[1], 0, BLOCK_LENGTH, false, mBlockSource);
          }
        });
    byte[] buffer = new byte[(int) BLOCK_LENGTH];
    try {
      mTestStream.read(buffer, 0, (int) BLOCK_LENGTH);
      fail("BlockInStream is inconsistent, an Exception is expected");
    } catch (IllegalStateException e) {
      // expect an exception to throw
    }
  }

  @Test
  public void getPos() throws Exception {
    assertEquals(0, mTestStream.getPos());
    mTestStream.read();
    assertEquals(1, mTestStream.getPos());
    mTestStream.read(new byte[(int) mFileSize], 0, (int) mFileSize);
    assertEquals(mFileSize, mTestStream.getPos());
  }

  // See https://github.com/Alluxio/alluxio/issues/13828
  @Test
  public void triggerAsyncOnClose() throws Exception {
    assumeTrue(mBlockSource == BlockInStreamSource.UFS);
    mInfo.setReplicationMax(1);
    mStatus = new URIStatus(mInfo);
    OpenFilePOptions readOptions =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mTestStream = new AlluxioFileInStream(mStatus, new InStreamOptions(mStatus, readOptions,
        mConf), mContext);
    mTestStream.read(new byte[(int) mFileSize], 0, (int) mFileSize);
    assertEquals(mFileSize, mTestStream.getPos());
    assertTrue(mTestStream.triggerAsyncCaching(mInStreams.get(mInStreams.size() - 1)));
  }

  /**
   * Tests that reading dataRead bytes into a buffer will properly write those bytes to the cache
   * streams and that the correct bytes are read from the {@link FileInStream}.
   *
   * @param dataRead the bytes to read
   */
  private void testReadBuffer(int dataRead) throws Exception {
    byte[] buffer = new byte[dataRead];
    mTestStream.read(buffer);
    assertArrayEquals(BufferUtils.getIncreasingByteArray(dataRead), buffer);
  }

  /**
   * Validates the partial caching behavior. This function
   * verifies the block at the given index is read for the given sizes.
   */
  // TODO(binfan): with better netty RPC mocking, verify that async cache request for the target
  // block is sent to the netty channel
  private void validatePartialCaching(int index, int readSize) {
    assertEquals(readSize, mInStreams.get(index).getBytesRead());
  }
}
