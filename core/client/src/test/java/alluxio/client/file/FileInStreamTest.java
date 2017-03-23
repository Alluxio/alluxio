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

import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.BufferedBlockInStream;
import alluxio.client.block.BufferedBlockOutStream;
import alluxio.client.block.StreamFactory;
import alluxio.client.block.TestBufferedBlockInStream;
import alluxio.client.block.TestBufferedBlockOutStream;
import alluxio.client.block.UnderFileSystemBlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for the {@link FileInStream} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, AlluxioBlockStore.class, StreamFactory.class,
    UnderFileSystemBlockInStream.class})
public class FileInStreamTest {

  private static final long BLOCK_LENGTH = 100L;
  private static final long FILE_LENGTH = 350L;
  private static final long NUM_STREAMS = ((FILE_LENGTH - 1) / BLOCK_LENGTH) + 1;

  private AlluxioBlockStore mBlockStore;
  private FileSystemContext mContext;
  private FileInfo mInfo;
  private URIStatus mStatus;

  private List<TestBufferedBlockOutStream> mCacheStreams;

  private FileInStream mTestStream;

  private long getBlockLength(int streamId) {
    return streamId == NUM_STREAMS - 1 ? 50 : BLOCK_LENGTH;
  }

  /**
   * Sets up the context and streams before a test runs.
   *
   * @throws AlluxioException when the worker ufs operations fail
   * @throws IOException when the read and write streams fail
   */
  @Before
  public void before() throws Exception {
    mInfo = new FileInfo().setBlockSizeBytes(BLOCK_LENGTH).setLength(FILE_LENGTH);

    ClientTestUtils.setSmallBufferSizes();

    mContext = PowerMockito.mock(FileSystemContext.class);
    mBlockStore = Mockito.mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mContext)).thenReturn(mBlockStore);
    PowerMockito.when(mBlockStore.getWorkerInfoList()).thenReturn(new ArrayList<BlockWorkerInfo>());
    Mockito.mock(StreamFactory.class);
    PowerMockito.mockStatic(StreamFactory.class);

    // Set up BufferedBlockInStreams and caching streams
    mCacheStreams = new ArrayList<>();
    List<Long> blockIds = new ArrayList<>();
    for (int i = 0; i < NUM_STREAMS; i++) {
      blockIds.add((long) i);
      mCacheStreams.add(new TestBufferedBlockOutStream(i, getBlockLength(i), mContext));
      Mockito.when(mBlockStore.getWorkerInfoList())
          .thenReturn(Arrays.asList(new BlockWorkerInfo(new WorkerNetAddress(), 0, 0)));
      Mockito
          .when(mBlockStore.getInStream(Mockito.eq((long) i), Mockito.any(InStreamOptions.class)))
          .thenAnswer(new Answer<BufferedBlockInStream>() {
            @Override
            public BufferedBlockInStream answer(InvocationOnMock invocation) throws Throwable {
              long i = (Long) invocation.getArguments()[0];
              byte[] input = BufferUtils
                  .getIncreasingByteArray((int) (i * BLOCK_LENGTH), (int) getBlockLength((int) i));
              return new TestBufferedBlockInStream(i, input);
            }
          });
      Mockito.when(mBlockStore.getOutStream(Mockito.eq((long) i), Mockito.anyLong(),
          Mockito.any(WorkerNetAddress.class), Mockito.any(OutStreamOptions.class)))
          .thenAnswer(new Answer<BufferedBlockOutStream>() {
            @Override
            public BufferedBlockOutStream answer(InvocationOnMock invocation) throws Throwable {
              long i = (Long) invocation.getArguments()[0];
              return mCacheStreams.get((int) i).isClosed() ? null : mCacheStreams.get((int) i);
            }
          });
    }
    mInfo.setBlockIds(blockIds);
    mStatus = new URIStatus(mInfo);

    mTestStream =
        new FileInStream(mStatus, InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(false), mContext);
  }

  @After
  public void after() {
    ClientTestUtils.resetClient();
  }

  /**
   * Tests that reading through the file one byte at a time will yield the correct data.
   */
  @Test
  public void singleByteRead() throws Exception {
    for (int i = 0; i < FILE_LENGTH; i++) {
      Assert.assertEquals(i & 0xff, mTestStream.read());
    }
    verifyCacheStreams(FILE_LENGTH);
    mTestStream.close();
  }

  /**
   * Tests that reading half of a file works.
   */
  @Test
  public void readHalfFile() throws Exception {
    testReadBuffer((int) (FILE_LENGTH / 2));
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
   * Tests that reading the complete file works.
   */
  @Test
  public void readFile() throws Exception {
    testReadBuffer((int) FILE_LENGTH);
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
    Assert.assertArrayEquals(expectedBuffer, buffer);
  }

  /**
   * Read through the file in small chunks and verify each chunk.
   */
  @Test
  public void readManyChunks() throws IOException {
    int chunksize = 10;
    // chunksize must divide FILE_LENGTH evenly for this test to work
    Assert.assertEquals(0, FILE_LENGTH % chunksize);
    byte[] buffer = new byte[chunksize];
    int offset = 0;
    for (int i = 0; i < FILE_LENGTH / chunksize; i++) {
      mTestStream.read(buffer, 0, chunksize);
      Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(offset, chunksize), buffer);
      offset += chunksize;
    }
    verifyCacheStreams(FILE_LENGTH);
  }

  /**
   * Tests that {@link FileInStream#remaining()} is correctly updated during reads, skips, and
   * seeks.
   */
  @Test
  public void testRemaining() throws IOException {
    Assert.assertEquals(FILE_LENGTH, mTestStream.remaining());
    mTestStream.read();
    Assert.assertEquals(FILE_LENGTH - 1, mTestStream.remaining());
    mTestStream.read(new byte[150]);
    Assert.assertEquals(FILE_LENGTH - 151, mTestStream.remaining());
    mTestStream.skip(140);
    Assert.assertEquals(FILE_LENGTH - 291, mTestStream.remaining());
    mTestStream.seek(310);
    Assert.assertEquals(FILE_LENGTH - 310, mTestStream.remaining());
    mTestStream.seek(130);
    Assert.assertEquals(FILE_LENGTH - 130, mTestStream.remaining());
  }

  /**
   * Tests seek, particularly that seeking over part of a block will cause us not to cache it, and
   * cancels the existing cache stream.
   */
  @Test
  public void testSeek() throws IOException {
    int seekAmount = (int) (BLOCK_LENGTH / 2);
    int readAmount = (int) (BLOCK_LENGTH * 2);
    byte[] buffer = new byte[readAmount];
    // Seek halfway into block 1
    mTestStream.seek(seekAmount);
    // Read two blocks from 0.5 to 2.5
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(seekAmount, readAmount), buffer);
    // First block should not be cached since we skipped over it, but the second should be
    Assert.assertEquals(0, mCacheStreams.get(0).getWrittenData().length);
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH),
        mCacheStreams.get(1).getWrittenData());

    // Seek to current position (does nothing)
    mTestStream.seek(seekAmount + readAmount);
    // Seek a short way past start of block 3
    mTestStream.seek((long) (BLOCK_LENGTH * 3.1));
    Assert.assertEquals((byte) (BLOCK_LENGTH * 3.1), mTestStream.read());
    mTestStream.seek(FILE_LENGTH);
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks backward for more than a block.
   */
  @Test
  public void longSeekBackwardCachingPartiallyReadBlocks() throws IOException {
    mTestStream = new FileInStream(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(true), mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 4 + BLOCK_LENGTH);
    int readAmount = (int) (BLOCK_LENGTH * 3 - BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount - seekAmount);

    // Block 2 is cached though it is not fully read.
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray(2 * (int) BLOCK_LENGTH, (int) BLOCK_LENGTH),
        mCacheStreams.get(2).getWrittenData());
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks backward within 1 block.
   */
  @Test
  public void shortSeekBackwardCachingPartiallyReadBlocks() throws IOException {
    mTestStream = new FileInStream(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(true), mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 4);
    int readAmount = (int) (BLOCK_LENGTH * 2 - BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount - seekAmount);

    // Block 1 is cached though it is not fully read.
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH),
        mCacheStreams.get(1).getWrittenData());

    // Seek many times. It will cache block 1 only once.
    for (int i = 0; i <= seekAmount; i++) {
      mTestStream.seek(readAmount - seekAmount - i);
    }
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH),
        mCacheStreams.get(1).getWrittenData());
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks forward for more than a block.
   */
  @Test
  public void longSeekForwardCachingPartiallyReadBlocks() throws IOException {
    mTestStream = new FileInStream(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(true), mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 4 + BLOCK_LENGTH);
    int readAmount = (int) (BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount + seekAmount);

    // Block 0 is cached though it is not fully read.
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(0, (int) BLOCK_LENGTH),
        mCacheStreams.get(0).getWrittenData());
    // Block 1 is being cached though its prefix it not read.
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH / 4 * 3),
        mCacheStreams.get(1).getWrittenData());
    mTestStream.seek(FILE_LENGTH);
  }

  /**
   * Tests seeking with incomplete block caching enabled. It seeks forward within a block.
   */
  @Test
  public void shortSeekForwardCachingPartiallyReadBlocks() throws IOException {
    mTestStream = new FileInStream(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE)
            .setCachePartiallyReadBlock(true), mContext);
    int seekAmount = (int) (BLOCK_LENGTH / 4);
    int readAmount = (int) (BLOCK_LENGTH * 2 - BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount + seekAmount);

    // Block 1 (till seek pos) is being cached.
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH / 4 * 3),
        mCacheStreams.get(1).getWrittenData());

    // Seek forward many times. The prefix is always cached.
    for (int i = 0; i < seekAmount; i++) {
      mTestStream.seek(readAmount + seekAmount + i);
      Assert.assertArrayEquals(BufferUtils
              .getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH / 2 + seekAmount + i),
          mCacheStreams.get(1).getWrittenData());
    }
  }

  /**
   * Tests skipping backwards when the seek buffer size is smaller than block size.
   */
  @Test
  public void seekBackwardSmallSeekBuffer() throws IOException {
    mTestStream = new FileInStream(mStatus,
        InStreamOptions.defaults().setCachePartiallyReadBlock(true)
            .setReadType(ReadType.CACHE_PROMOTE).setSeekBufferSizeBytes(7), mContext);
    int readAmount = (int) (BLOCK_LENGTH / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    mTestStream.seek(readAmount - 1);

    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(0, (int) BLOCK_LENGTH),
        mCacheStreams.get(0).getWrittenData());
    Assert.assertEquals(0, mCacheStreams.get(1).getWrittenData().length);
  }

  /**
   * Tests skip, particularly that skipping the start of a block will cause us not to cache it, and
   * cancels the existing cache stream.
   */
  @Test
  public void testSkip() throws IOException {
    int skipAmount = (int) (BLOCK_LENGTH / 2);
    int readAmount = (int) (BLOCK_LENGTH * 2);
    byte[] buffer = new byte[readAmount];
    // Skip halfway into block 1
    mTestStream.skip(skipAmount);
    // Read two blocks from 0.5 to 2.5
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(skipAmount, readAmount), buffer);
    // First block should not be cached since we skipped into it, but the second should be
    Assert.assertEquals(0, mCacheStreams.get(0).getWrittenData().length);
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH),
        mCacheStreams.get(1).getWrittenData());

    Assert.assertEquals(0, mTestStream.skip(0));
    // Skip the next half block, bringing us to block 3
    Assert.assertEquals(BLOCK_LENGTH / 2, mTestStream.skip(BLOCK_LENGTH / 2));
    Assert.assertEquals((byte) (BLOCK_LENGTH * 3), mTestStream.read());
  }

  /**
   * Tests that we promote blocks when they are read.
   */
  @Test
  public void testPromote() throws IOException {
    Mockito.verify(mBlockStore, Mockito.times(0)).promote(0);
    mTestStream.read();
    Mockito.verify(mBlockStore).promote(0);
    mTestStream.read();
    Mockito.verify(mBlockStore).promote(0);
    Mockito.verify(mBlockStore, Mockito.times(0)).promote(1);
    mTestStream.read(new byte[(int) BLOCK_LENGTH]);
    Mockito.verify(mBlockStore).promote(1);
  }

  /**
   * Tests that {@link IOException}s thrown by the {@link AlluxioBlockStore} are properly
   * propagated.
   */
  @Test
  public void failGetInStream() throws IOException {
    Mockito.when(mBlockStore.getInStream(Mockito.eq(1L), Mockito.any(InStreamOptions.class)))
        .thenThrow(new IOException("test IOException"));

    try {
      mTestStream.seek(BLOCK_LENGTH);
      Assert.fail("block store should throw exception");
    } catch (IOException e) {
      Assert.assertEquals("test IOException", e.getMessage());
    }
  }

  /**
   * Tests the capability to fall back to a ufs stream when getting an alluxio stream fails.
   */
  @Test
  public void failToUnderFs() throws AlluxioException, IOException {
    mInfo.setPersisted(true).setUfsPath("testUfsPath");
    mStatus = new URIStatus(mInfo);
    mTestStream =
        new FileInStream(mStatus, InStreamOptions.defaults().setCachePartiallyReadBlock(false),
            mContext);

    Mockito.when(mBlockStore.getInStream(Mockito.eq(1L), Mockito.any(InStreamOptions.class)))
        .thenThrow(new IOException("test IOException"));
    UnderFileSystemBlockInStream inStream = PowerMockito.mock(UnderFileSystemBlockInStream.class);
    PowerMockito.when(
        StreamFactory.createUfsBlockInStream(Mockito.any(FileSystemContext.class),
            Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(),
            Mockito.any(WorkerNetAddress.class), Mockito.any(InStreamOptions.class))).thenReturn(
        inStream);
    mTestStream.seek(BLOCK_LENGTH + (BLOCK_LENGTH / 2));
  }

  /**
   * Tests that reading out of bounds properly returns -1.
   */
  @Test
  public void readOutOfBounds() throws IOException {
    mTestStream.read(new byte[(int) FILE_LENGTH]);
    Assert.assertEquals(-1, mTestStream.read());
    Assert.assertEquals(-1, mTestStream.read(new byte[10]));
  }

  /**
   * Tests that specifying an invalid offset/length for a buffer read throws the right exception.
   */
  @Test
  public void readBadBuffer() throws IOException {
    try {
      mTestStream.read(new byte[10], 5, 6);
      Assert.fail("the buffer read of invalid offset/length should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_BUFFER_STATE.toString(), 10, 5, 6),
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
      Assert.fail("seeking negative position should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), -1),
          e.getMessage());
    }
  }

  /**
   * Tests that seeking past the end of the stream will throw the right exception.
   */
  @Test
  public void seekPastEnd() throws IOException {
    try {
      mTestStream.seek(FILE_LENGTH + 1);
      Assert.fail("seeking past the end of the stream should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(),
          FILE_LENGTH + 1), e.getMessage());
    }
  }

  /**
   * Tests that skipping a negative amount correctly reports that 0 bytes were skipped.
   */
  @Test
  public void skipNegative() throws IOException {
    Assert.assertEquals(0, mTestStream.skip(-10));
  }

  /**
   * Tests that the file in stream uses the supplied location policy.
   */
  @Test
  public void locationPolicy() throws Exception {
    FileWriteLocationPolicy policy = Mockito.mock(FileWriteLocationPolicy.class);
    mTestStream = new FileInStream(mStatus,
        InStreamOptions.defaults().setReadType(ReadType.CACHE).setLocationPolicy(policy), mContext);
    mTestStream.read();
    Mockito.verify(policy)
        .getWorkerForNextBlock(Mockito.anyListOf(BlockWorkerInfo.class), Mockito.anyLong());
  }

  /**
   * Tests that the correct exception message is produced when the location policy is not specified.
   */
  @Test
  public void missingLocationPolicy() {
    try {
      mTestStream = new FileInStream(mStatus,
          InStreamOptions.defaults().setReadType(ReadType.CACHE).setLocationPolicy(null), mContext);
    } catch (NullPointerException e) {
      Assert.assertEquals(PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED.toString(),
          e.getMessage());
    }
  }

  /**
   * Tests that reading dataRead bytes into a buffer will properly write those bytes to the cache
   * streams and that the correct bytes are read from the {@link FileInStream}.
   *
   * @param dataRead the bytes to read
   * @throws Exception when reading from the stream fails
   */
  private void testReadBuffer(int dataRead) throws Exception {
    byte[] buffer = new byte[dataRead];
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(dataRead), buffer);

    verifyCacheStreams(dataRead);
  }

  /**
   * Verifies that data was properly written to the cache streams.
   *
   * @param dataRead the bytes to read
   */
  private void verifyCacheStreams(long dataRead) {
    for (int streamIndex = 0; streamIndex < NUM_STREAMS; streamIndex++) {
      TestBufferedBlockOutStream stream = mCacheStreams.get(streamIndex);
      byte[] data = stream.getWrittenData();
      if (streamIndex * BLOCK_LENGTH > dataRead) {
        Assert.assertEquals(0, data.length);
      } else {
        long dataStart = streamIndex * BLOCK_LENGTH;
        for (int i = 0; i < BLOCK_LENGTH && dataStart + i < dataRead; i++) {
          Assert.assertEquals((byte) (dataStart + i), data[i]);
        }
      }
    }
  }
}
