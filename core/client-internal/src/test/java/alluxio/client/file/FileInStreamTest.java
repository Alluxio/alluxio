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

package alluxio.client.file;

import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockInStream;
import alluxio.client.block.BufferedBlockInStream;
import alluxio.client.block.TestBufferedBlockInStream;
import alluxio.client.block.TestBufferedBlockOutStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.client.util.ClientMockUtils;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
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
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Tests for the {@link FileInStream} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, AlluxioBlockStore.class, UnderFileSystem.class})
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

  /**
   * Sets up the context and streams before a test runs.
   *
   * @throws IOException when the read and write streams fail
   */
  @Before
  public void before() throws IOException {
    mInfo = new FileInfo().setBlockSizeBytes(BLOCK_LENGTH).setLength(FILE_LENGTH);

    ClientTestUtils.setSmallBufferSizes();

    mContext = PowerMockito.mock(FileSystemContext.class);
    mBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    Mockito.when(mContext.getAluxioBlockStore()).thenReturn(mBlockStore);

    // Set up BufferedBlockInStreams and caching streams
    mCacheStreams = Lists.newArrayList();
    List<Long> blockIds = Lists.newArrayList();
    for (int i = 0; i < NUM_STREAMS; i++) {
      blockIds.add((long) i);
      mCacheStreams.add(new TestBufferedBlockOutStream(i, BLOCK_LENGTH));
      Mockito.when(mBlockStore.getInStream(i)).thenAnswer(new Answer<BufferedBlockInStream>() {
        @Override
        public BufferedBlockInStream answer(InvocationOnMock invocation) throws Throwable {
          long i = (Long) invocation.getArguments()[0];
          return new TestBufferedBlockInStream(i, (int) (i * BLOCK_LENGTH), BLOCK_LENGTH);
        }
      });

      Mockito.when(mBlockStore.getOutStream(Mockito.eq((long) i), Mockito.anyLong(),
          Mockito.any(WorkerNetAddress.class))).thenReturn(mCacheStreams.get(i));
    }
    mInfo.setBlockIds(blockIds);
    mStatus = new URIStatus(mInfo);

    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", mContext);
    mTestStream =
        new FileInStream(mStatus, InStreamOptions.defaults().setReadType(
            ReadType.CACHE_PROMOTE));
  }

  @After
  public void after() {
    ClientTestUtils.resetClientContext();
  }

  /**
   * Tests that reading through the file one byte at a time will yield the correct data.
   *
   * @throws Exception when reading from the stream or closing the stream fails
   */
  @Test
  public void singleByteReadTest() throws Exception {
    for (int i = 0; i < FILE_LENGTH; i++) {
      Assert.assertEquals(i & 0xff, mTestStream.read());
    }
    verifyCacheStreams(FILE_LENGTH);
    mTestStream.close();
    Assert.assertTrue((Boolean) Whitebox.getInternalState(mTestStream, "mClosed"));
  }

  /**
   * Tests that reading half of a file works.
   *
   * @throws Exception when reading from the stream fails
   */
  @Test
  public void readHalfFileTest() throws Exception {
    testReadBuffer((int) (FILE_LENGTH / 2));
  }

  /**
   * Tests that reading a part of a file works.
   *
   * @throws Exception when reading from the stream fails
   */
  @Test
  public void readPartialBlockTest() throws Exception {
    testReadBuffer((int) (BLOCK_LENGTH / 2));
  }

  /**
   * Tests that reading the complete block works.
   *
   * @throws Exception when reading from the stream fails
   */
  @Test
  public void readBlockTest() throws Exception {
    testReadBuffer((int) BLOCK_LENGTH);
  }

  /**
   * Tests that reading the complete file works.
   *
   * @throws Exception when reading from the stream fails
   */
  @Test
  public void readFileTest() throws Exception {
    testReadBuffer((int) FILE_LENGTH);
  }

  /**
   * Tests that reading a buffer at an offset writes the bytes to the correct places.
   *
   * @throws IOException when reading from the stream fails
   */
  @Test
  public void readOffsetTest() throws IOException {
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
   *
   * @throws IOException when reading from the stream fails
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
   *
   * @throws IOException when reading from the stream fails
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
   *
   * @throws IOException when reading from the stream fails
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
    Assert.assertTrue(mCacheStreams.get(0).isCanceled());
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH),
        mCacheStreams.get(1).getWrittenData());

    // Seek to current position (does nothing)
    mTestStream.seek(seekAmount + readAmount);
    // Seek a short way past start of block 3
    mTestStream.seek((long) (BLOCK_LENGTH * 3.1));
    Assert.assertEquals((byte) (BLOCK_LENGTH * 3.1), mTestStream.read());
  }

  /**
   * Tests skip, particularly that skipping the start of a block will cause us not to cache it, and
   * cancels the existing cache stream.
   *
   * @throws IOException when an operation on the stream fails
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
    Assert.assertTrue(mCacheStreams.get(0).isCanceled());
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
   *
   * @throws IOException when reading from the stream fails
   */
  @Test
  public void testPromote() throws IOException {
    Mockito.verify(mBlockStore, Mockito.times(0)).promote(0);
    mTestStream.read();
    Mockito.verify(mBlockStore, Mockito.times(1)).promote(0);
    mTestStream.read();
    Mockito.verify(mBlockStore, Mockito.times(1)).promote(0);
    Mockito.verify(mBlockStore, Mockito.times(0)).promote(1);
    mTestStream.read(new byte[(int) BLOCK_LENGTH]);
    Mockito.verify(mBlockStore, Mockito.times(1)).promote(1);
  }

  /**
   * Tests that {@link IOException}s thrown by the {@link AlluxioBlockStore} are properly
   * propagated.
   *
   * @throws IOException when an operation on the BlockStore or on the stream fails
   */
  @Test
  public void failGetInStreamTest() throws IOException {
    Mockito.when(mBlockStore.getInStream(1L)).thenThrow(new IOException("test IOException"));

    try {
      mTestStream.seek(BLOCK_LENGTH);
      Assert.fail("block store should throw exception");
    } catch (IOException e) {
      Assert.assertEquals("test IOException", e.getMessage());
    }
  }

  /**
   * Tests the capability to fall back to a ufs stream when getting an alluxio stream fails.
   *
   * @throws IOException when an operation on the stream fails
   */
  @Test
  public void failToUnderFsTest() throws IOException {
    mInfo.setPersisted(true).setUfsPath("testUfsPath");
    mStatus = new URIStatus(mInfo);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", mContext);
    mTestStream = new FileInStream(mStatus, InStreamOptions.defaults());

    Mockito.when(mBlockStore.getInStream(1L)).thenThrow(new IOException("test IOException"));
    UnderFileSystem ufs = ClientMockUtils.mockUnderFileSystem(Mockito.eq("testUfsPath"));
    InputStream stream = Mockito.mock(InputStream.class);
    Mockito.when(ufs.open("testUfsPath")).thenReturn(stream);
    Mockito.when(stream.skip(BLOCK_LENGTH)).thenReturn(BLOCK_LENGTH);
    Mockito.when(stream.skip(BLOCK_LENGTH / 2)).thenReturn(BLOCK_LENGTH / 2);

    mTestStream.seek(BLOCK_LENGTH + (BLOCK_LENGTH / 2));
    Mockito.verify(ufs, Mockito.times(1)).open("testUfsPath");
    Mockito.verify(stream, Mockito.times(1)).skip(100);
    Mockito.verify(stream, Mockito.times(1)).skip(50);
  }

  /**
   * Tests that seeking into the middle of a block will invalidate caching for that block.
   *
   * @throws IOException when seeking from the stream fails
   */
  @Test
  public void dontCacheMidBlockSeekTest() throws IOException {
    mTestStream.seek(BLOCK_LENGTH + (BLOCK_LENGTH / 2));
    // Shouldn't cache the current block when we skipped its beginning
    Assert
        .assertFalse((Boolean) Whitebox.getInternalState(mTestStream, "mShouldCacheCurrentBlock"));
  }

  /**
   * Tests that reading out of bounds properly returns -1.
   *
   * @throws IOException when reading from the stream fails
   */
  @Test
  public void readOutOfBoundsTest() throws IOException {
    mTestStream.read(new byte[(int) FILE_LENGTH]);
    Assert.assertEquals(-1, mTestStream.read());
    Assert.assertEquals(-1, mTestStream.read(new byte[10]));
  }

  /**
   * Tests that specifying an invalid offset/length for a buffer read throws the right exception.
   *
   * @throws IOException when reading from the stream fails
   */
  @Test
  public void readBadBufferTest() throws IOException {
    try {
      mTestStream.read(new byte[10], 5, 6);
      Assert.fail("the buffer read of invalid offset/length should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_BUFFER_STATE, 10, 5, 6),
          e.getMessage());
    }
  }

  /**
   * Tests that seeking to a negative position will throw the right exception.
   *
   * @throws IOException when seeking from the stream fails
   */
  @Test
  public void seekNegativeTest() throws IOException {
    try {
      mTestStream.seek(-1);
      Assert.fail("seeking negative position should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE, -1), e.getMessage());
    }
  }

  /**
   * Tests that seeking past the end of the stream will throw the right exception.
   *
   * @throws IOException when seeking from the stream fails
   */
  @Test
  public void seekPastEndTest() throws IOException {
    try {
      mTestStream.seek(FILE_LENGTH + 1);
      Assert.fail("seeking past the end of the stream should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(
          String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE, FILE_LENGTH + 1),
          e.getMessage());
    }
  }

  /**
   * Tests that skipping a negative amount correctly reports that 0 bytes were skipped.
   *
   * @throws IOException when skipping on the stream fails
   */
  @Test
  public void skipNegativeTest() throws IOException {
    Assert.assertEquals(0, mTestStream.skip(-10));
  }

  /**
   * Tests that an {@link IOException} thrown by a {@link BlockInStream} during a skip will be
   * handled correctly.
   *
   * @throws IOException when an operation on the stream fails
   */
  @Test
  public void skipInstreamExceptionTest() throws IOException {
    long skipSize = BLOCK_LENGTH / 2;
    BlockInStream blockInStream = Mockito.mock(BlockInStream.class);
    Whitebox.setInternalState(mTestStream, "mCurrentBlockInStream", blockInStream);
    Mockito.when(blockInStream.skip(skipSize)).thenReturn(0L);
    Mockito.when(blockInStream.remaining()).thenReturn(BLOCK_LENGTH);

    try {
      mTestStream.skip(skipSize);
      Assert.fail("skip in instream should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.INSTREAM_CANNOT_SKIP.getMessage(skipSize),
          e.getMessage());
    }
  }

  /**
   * Tests the location policy created with different options.
   */
  @Test
  public void locationPolicyTest() {
    mTestStream =
        new FileInStream(mStatus, InStreamOptions.defaults().setReadType(ReadType.CACHE_PROMOTE));

    // by default local first policy used
    FileWriteLocationPolicy policy = Whitebox.getInternalState(mTestStream, "mLocationPolicy");
    Assert.assertTrue(policy instanceof LocalFirstPolicy);

    // configure a different policy
    mTestStream =
        new FileInStream(mStatus, InStreamOptions.defaults().setReadType(ReadType.CACHE)
            .setLocationPolicy(new RoundRobinPolicy()));
    policy = Whitebox.getInternalState(mTestStream, "mLocationPolicy");
    Assert.assertTrue(policy instanceof RoundRobinPolicy);
  }

  /**
   * Tests that the correct exception message is produced when the location policy is not specified.
   */
  @Test
  public void missingLocationPolicyTest() {
    try {
      mTestStream =
          new FileInStream(mStatus, InStreamOptions.defaults().setReadType(ReadType.CACHE)
              .setLocationPolicy(null));
    } catch (NullPointerException e) {
      Assert.assertEquals(PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED,
          e.getMessage());
    }
  }

  /**
   * Tests cache streams are created with the proper block sizes when the file size is smaller
   * than block size and we skip before reading.
   */
  @Test
  public void cacheStreamBlockSizeTest() throws Exception {
    long smallSize = BLOCK_LENGTH / 2;
    mInfo.setLength(smallSize);
    mTestStream =
        new FileInStream(new URIStatus(mInfo), InStreamOptions.defaults().setReadType(
            ReadType.CACHE));
    mTestStream.skip(smallSize / 2);
    mTestStream.read(new byte[1]);
    Mockito.verify(mBlockStore, Mockito.times(1)).getOutStream(Mockito.anyLong(),
        Mockito.eq(smallSize), Mockito.any(WorkerNetAddress.class));
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
