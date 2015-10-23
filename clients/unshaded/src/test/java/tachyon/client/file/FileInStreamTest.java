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

package tachyon.client.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.TachyonStorageType;
import tachyon.client.block.BlockInStream;
import tachyon.client.block.BufferedBlockInStream;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.block.TestBufferedBlockInStream;
import tachyon.client.block.TestBufferedBlockOutStream;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.util.ClientMockUtils;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.PreconditionMessage;
import tachyon.test.Tester;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.BufferUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, TachyonBlockStore.class, UnderFileSystem.class})
public class FileInStreamTest implements Tester<FileInStream> {

  private static final long BLOCK_LENGTH = 100L;
  private static final long FILE_LENGTH = 350L;
  private static final long NUM_STREAMS = ((FILE_LENGTH - 1) / BLOCK_LENGTH) + 1;

  private TachyonBlockStore mBlockStore;
  private TachyonConf mConf;
  private FileSystemContext mContext;
  private FileInfo mInfo;

  private List<BufferedBlockInStream> mInStreams;
  private List<TestBufferedBlockOutStream> mCacheStreams;

  private FileInStream.PrivateAccess mPrivateAccess;
  private FileInStream mTestStream;

  @Override
  public void receiveAccess(Object access) {
    mPrivateAccess = (FileInStream.PrivateAccess) access;
  }

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws IOException {
    mInfo = new FileInfo().setBlockSizeBytes(BLOCK_LENGTH).setLength(FILE_LENGTH);

    // Set small buffer sizes so that we don't run out of heap space
    mConf = new TachyonConf();
    mConf.set(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "4KB");
    mConf.set(Constants.USER_FILE_BUFFER_BYTES, "4KB");
    ClientContext.reset(mConf);

    mContext = PowerMockito.mock(FileSystemContext.class);
    mBlockStore = PowerMockito.mock(TachyonBlockStore.class);
    Mockito.when(mContext.getTachyonBlockStore()).thenReturn(mBlockStore);

    // Set up BufferedBlockInStreams and caching streams
    mInStreams = Lists.newArrayList();
    mCacheStreams = Lists.newArrayList();
    List<Long> blockIds = Lists.newArrayList();
    for (int i = 0; i < NUM_STREAMS; i ++) {
      blockIds.add((long) i);
      mInStreams.add(new TestBufferedBlockInStream(i, (int) (i * BLOCK_LENGTH), BLOCK_LENGTH));
      mCacheStreams.add(new TestBufferedBlockOutStream(i, BLOCK_LENGTH));
      Mockito.when(mBlockStore.getInStream(i)).thenReturn(mInStreams.get(i));
      Mockito
          .when(
              mBlockStore.getOutStream(Mockito.eq((long) i), Mockito.eq(-1L), Mockito.anyString()))
          .thenReturn(mCacheStreams.get(i));
    }
    mInfo.setBlockIds(blockIds);

    mTestStream = new FileInStream(mInfo, new InStreamOptions.Builder(mConf)
        .setTachyonStorageType(TachyonStorageType.PROMOTE).build(), mContext);
    mTestStream.grantAccess(FileInStreamTest.this);
  }

  @Test
  public void singleByteReadTest() throws Exception {
    // Verify byte by byte read is equal to increasing byte array
    for (int i = 0; i < FILE_LENGTH; i ++) {
      Assert.assertEquals((byte) i, (byte) mTestStream.read());
    }
    verifyCacheStreams(FILE_LENGTH);
    mTestStream.close();
    Assert.assertTrue(mPrivateAccess.isClosed());
  }

  @Test
  public void readFileTest() throws Exception {
    testReadBuffer((int) FILE_LENGTH);
  }

  @Test
  public void readHalfFileTest() throws Exception {
    testReadBuffer((int) (FILE_LENGTH / 2));
  }

  @Test
  public void readPartialBlockTest() throws Exception {
    testReadBuffer((int) (BLOCK_LENGTH / 2));
  }

  @Test
  public void readBlockTest() throws Exception {
    testReadBuffer((int) BLOCK_LENGTH);
  }

  @Test
  public void readOffsetTest() throws IOException {
    int offset = (int) (BLOCK_LENGTH / 3);
    int len = (int) BLOCK_LENGTH;
    byte[] buffer = new byte[offset + len];
    Arrays.fill(buffer, (byte) 0);
    byte[] expectedBuffer = new byte[offset + len];
    Arrays.fill(expectedBuffer, (byte) 0);
    System.arraycopy(BufferUtils.getIncreasingByteArray(len), 0, expectedBuffer, offset, len);
    mTestStream.read(buffer, offset, len);
    Assert.assertArrayEquals(expectedBuffer, buffer);
  }

  @Test
  public void readManyChunks() throws IOException {
    int chunksize = 10;
    Assert.assertEquals(0, FILE_LENGTH % chunksize);
    byte[] buffer = new byte[chunksize];
    int offset = 0;
    for (int i = 0; i < FILE_LENGTH / chunksize; i ++) {
      mTestStream.read(buffer, 0, chunksize);
      Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(offset, chunksize), buffer);
      offset += chunksize;
    }
    verifyCacheStreams(FILE_LENGTH);
  }

  @Test
  public void testRemaining() throws IOException {
    Assert.assertEquals(FILE_LENGTH, mTestStream.remaining());
    mTestStream.read();
    Assert.assertEquals(FILE_LENGTH - 1, mTestStream.remaining());
    mTestStream.read(new byte[150]);
    Assert.assertEquals(FILE_LENGTH - 151, mTestStream.remaining());
  }

  @Test
  public void testSeek() throws IOException {
    int seekAmount = (int) (BLOCK_LENGTH / 2);
    int readAmount = (int) (BLOCK_LENGTH * 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.seek(seekAmount);
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(seekAmount, readAmount), buffer);
    // First block should not be cached since we skipped over it, but the second should be
    Assert.assertTrue(mCacheStreams.get(0).isCanceled());
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH),
        mCacheStreams.get(1).getDataWritten());

    mTestStream.seek(seekAmount + readAmount);
    mTestStream.seek((long) (BLOCK_LENGTH * 3.1));
    Assert.assertEquals((byte) (BLOCK_LENGTH * 3.1), mTestStream.read());
  }

  @Test
  public void testSkip() throws IOException {
    int skipAmount = (int) (BLOCK_LENGTH / 2);
    int readAmount = (int) (BLOCK_LENGTH * 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.skip(skipAmount);
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(skipAmount, readAmount), buffer);
    // First block should not be cached since we skipped over it, but the second should be
    Assert.assertTrue(mCacheStreams.get(0).isCanceled());
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH, (int) BLOCK_LENGTH),
        mCacheStreams.get(1).getDataWritten());

    Assert.assertEquals(0, mTestStream.skip(0));
    Assert.assertEquals(BLOCK_LENGTH / 2, mTestStream.skip(BLOCK_LENGTH / 2));
    Assert.assertEquals((byte) (BLOCK_LENGTH * 3), mTestStream.read());
  }

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

  @Test
  public void failGetInStreamTest() throws IOException {
    Mockito.when(mBlockStore.getInStream(1L)).thenThrow(new IOException("test IOException"));

    mThrown.expect(IOException.class);
    mThrown.expectMessage("test IOException");
    mTestStream.seek(BLOCK_LENGTH);
  }

  @Test
  public void failToUnderFsTest() throws IOException {
    mInfo.setIsPersisted(true).setUfsPath("testUfsPath");
    mTestStream = new FileInStream(mInfo, InStreamOptions.defaults(), mContext);

    Mockito.when(mBlockStore.getInStream(1L)).thenThrow(new IOException("test IOException"));
    UnderFileSystem ufs = ClientMockUtils.mockUnderFileSystem(Mockito.eq("testUfsPath"));
    InputStream stream = Mockito.mock(InputStream.class);
    Mockito.when(ufs.open("testUfsPath")).thenReturn(stream);
    Mockito.when(stream.skip(BLOCK_LENGTH)).thenReturn(BLOCK_LENGTH);
    Mockito.when(stream.skip(BLOCK_LENGTH / 2)).thenReturn(BLOCK_LENGTH / 2);

    mTestStream.seek(BLOCK_LENGTH + (BLOCK_LENGTH / 2));
    Mockito.verify(stream, Mockito.times(1)).skip(100);
    Mockito.verify(stream, Mockito.times(1)).skip(50);
    Assert.assertTrue(mPrivateAccess.shouldCacheCurrentBlock());
  }

  @Test
  public void readOutOfBoundsTest() throws IOException {
    mTestStream.read(new byte[(int) FILE_LENGTH]);
    Assert.assertEquals(-1, mTestStream.read());
    Assert.assertEquals(-1, mTestStream.read(new byte[10]));
  }

  @Test
  public void readBadBufferTest() throws IOException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_BUFFER_STATE, 10, 5, 6));
    mTestStream.read(new byte[10], 5, 6);
  }

  @Test
  public void seekNegativeTest() throws IOException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE, -1));
    mTestStream.seek(-1);
  }

  @Test
  public void seekPastEndTest() throws IOException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(
        String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE, FILE_LENGTH + 1));
    mTestStream.seek(FILE_LENGTH + 1);
  }

  @Test
  public void skipNegativeTest() throws IOException {
    Assert.assertEquals(0, mTestStream.skip(-10));
  }

  @Test
  public void skipInstreamExceptionTest() throws IOException {
    long skipSize = BLOCK_LENGTH / 2;
    BlockInStream blockInStream = Mockito.mock(BlockInStream.class);
    mPrivateAccess.setCurrentInstream(blockInStream);
    Mockito.when(blockInStream.skip(skipSize)).thenReturn(0L);
    Mockito.when(blockInStream.remaining()).thenReturn(BLOCK_LENGTH);

    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.INSTREAM_CANNOT_SKIP.getMessage(skipSize));
    mTestStream.skip(skipSize);
  }

  private void testReadBuffer(int dataRead) throws Exception {
    byte[] buffer = new byte[dataRead];
    Arrays.fill(buffer, (byte) 0);
    byte[] expectedBuffer = new byte[dataRead];
    Arrays.fill(expectedBuffer, (byte) 0);
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(dataRead), buffer);

    verifyCacheStreams(dataRead);
  }

  /**
   * Verify that data was properly written to the cache streams
   */
  private void verifyCacheStreams(long dataRead) {
    for (int streamIndex = 0; streamIndex < NUM_STREAMS; streamIndex ++) {
      TestBufferedBlockOutStream stream = mCacheStreams.get(streamIndex);
      byte[] data = stream.getDataWritten();
      if (streamIndex * BLOCK_LENGTH > dataRead) {
        Assert.assertEquals(0, data.length);
      } else {
        long dataStart = streamIndex * BLOCK_LENGTH;
        for (int i = 0; i < BLOCK_LENGTH && dataStart + i < dataRead; i ++) {
          Assert.assertEquals((byte) (dataStart + i), data[i]);
        }
      }
    }
  }
}
