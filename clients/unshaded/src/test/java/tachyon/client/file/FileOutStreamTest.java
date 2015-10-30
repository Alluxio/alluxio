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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Maps;

import tachyon.client.ClientContext;
import tachyon.client.FileSystemMasterClient;
import tachyon.client.UnderStorageType;
import tachyon.client.block.BlockStoreContext;
import tachyon.client.block.BufferedBlockOutStream;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.block.TestBufferedBlockOutStream;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.client.util.ClientMockUtils;
import tachyon.client.util.ClientTestUtils;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.PreconditionMessage;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.BufferUtils;
import tachyon.worker.WorkerClient;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, BlockStoreContext.class, FileSystemMasterClient.class,
    TachyonBlockStore.class, UnderFileSystem.class, WorkerClient.class})
public class FileOutStreamTest {

  private static final long BLOCK_LENGTH = 100L;
  private static final long FILE_ID = 20L;

  private TachyonBlockStore mBlockStore;
  private BlockStoreContext mBlockStoreContext;
  private FileSystemContext mFileSystemContext;
  private FileSystemMasterClient mFileSystemMasterClient;
  private UnderFileSystem mUnderFileSystem;
  private WorkerClient mWorkerClient;

  private Map<Long, TestBufferedBlockOutStream> mTachyonOutStreamMap;
  private ByteArrayOutputStream mUnderStorageOutputStream;
  private AtomicBoolean mUnderStorageFlushed;

  private FileOutStream mTestStream;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    ClientTestUtils.setSmallBufferSizes();

    // PowerMock enums and final classes
    mFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mBlockStore = PowerMockito.mock(TachyonBlockStore.class);
    mBlockStoreContext = PowerMockito.mock(BlockStoreContext.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    mWorkerClient = PowerMockito.mock(WorkerClient.class);

    Mockito.when(mFileSystemContext.getTachyonBlockStore()).thenReturn(mBlockStore);
    Mockito.when(mBlockStoreContext.acquireWorkerClient()).thenReturn(mWorkerClient);
    Mockito.when(mFileSystemContext.acquireMasterClient()).thenReturn(mFileSystemMasterClient);
    Mockito.when(mFileSystemMasterClient.getFileInfo(Mockito.anyLong())).thenReturn(new FileInfo());

    // Return sequentially increasing numbers for new block ids
    Mockito.when(mFileSystemMasterClient.getNewBlockIdForFile(FILE_ID))
        .thenAnswer(new Answer<Long>() {
          private long mCount = 0;

          @Override
          public Long answer(InvocationOnMock invocation) throws Throwable {
            return mCount ++;
          }
        });

    // Set up out streams. When they are created, add them to outStreamMap
    final Map<Long, TestBufferedBlockOutStream> outStreamMap = Maps.newHashMap();
    Mockito.when(
        mBlockStore.getOutStream(Mockito.anyLong(), Mockito.eq(BLOCK_LENGTH), Mockito.anyString()))
        .thenAnswer(new Answer<BufferedBlockOutStream>() {
          @Override
          public BufferedBlockOutStream answer(InvocationOnMock invocation) throws Throwable {
            Long blockId = invocation.getArgumentAt(0, Long.class);
            if (!outStreamMap.containsKey(blockId)) {
              TestBufferedBlockOutStream newStream =
                  new TestBufferedBlockOutStream(blockId, BLOCK_LENGTH);
              outStreamMap.put(blockId, newStream);
            }
            return outStreamMap.get(blockId);
          }
        });
    mTachyonOutStreamMap = outStreamMap;

    // Create an under storage stream so that we can check whether it has been flushed
    final AtomicBoolean underStorageFlushed = new AtomicBoolean(false);
    mUnderStorageOutputStream = new ByteArrayOutputStream() {
      @Override
      public void flush() {
        underStorageFlushed.set(true);
      }
    };
    mUnderStorageFlushed = underStorageFlushed;

    // Set up underFileStorage so that we can test UnderStorageType.SYNC_PERSIST
    mUnderFileSystem = ClientMockUtils.mockUnderFileSystem();
    Mockito.when(mUnderFileSystem.create(Mockito.anyString(), Mockito.eq((int) BLOCK_LENGTH)))
        .thenReturn(mUnderStorageOutputStream);

    OutStreamOptions options = new OutStreamOptions.Builder(ClientContext.getConf())
        .setBlockSizeBytes(BLOCK_LENGTH).setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
    mTestStream = createTestStream(FILE_ID, options);
  }

  /**
   * Tests that a single byte is written to the out stream correctly.
   */
  @Test
  public void singleByteWriteTest() throws Exception {
    mTestStream.write(5);
    Assert.assertArrayEquals(new byte[] {5}, mTachyonOutStreamMap.get(0L).getDataWritten());
  }

  /**
   * Tests that many bytes, written one at a time, are written to the out streams correctly.
   */
  @Test
  public void manyBytesWriteTest() throws IOException {
    int bytesToWrite = (int) ((BLOCK_LENGTH * 5) + (BLOCK_LENGTH / 2));
    for (int i = 0; i < bytesToWrite; i ++) {
      mTestStream.write(i);
    }
    verifyIncreasingBytesWritten(bytesToWrite);
  }

  /**
   * Tests that writing a buffer all at once will write bytes to the out streams correctly.
   */
  @Test
  public void writeBufferTest() throws IOException {
    int bytesToWrite = (int) ((BLOCK_LENGTH * 5) + (BLOCK_LENGTH / 2));
    mTestStream.write(BufferUtils.getIncreasingByteArray(bytesToWrite));
    verifyIncreasingBytesWritten(bytesToWrite);
  }

  /**
   * Tests writing a buffer at an offset.
   */
  @Test
  public void writeOffsetTest() throws IOException {
    int bytesToWrite = (int) ((BLOCK_LENGTH * 5) + (BLOCK_LENGTH / 2));
    int offset = (int) (BLOCK_LENGTH / 3);
    mTestStream.write(BufferUtils.getIncreasingByteArray(bytesToWrite + offset), offset,
        bytesToWrite);
    verifyIncreasingBytesWritten(offset, bytesToWrite);
  }

  /**
   * Tests that close() will close but not cancel the underlying out streams. Also checks that
   * close() persists and completes the file.
   */
  @Test
  public void closeTest() throws Exception {
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.close();
    for (long streamIndex = 0; streamIndex < 2; streamIndex ++) {
      Assert.assertFalse(mTachyonOutStreamMap.get(streamIndex).isCanceled());
      Assert.assertTrue(mTachyonOutStreamMap.get(streamIndex).isClosed());
    }
    Mockito.verify(mWorkerClient, Mockito.times(1)).persistFile(Mockito.eq(FILE_ID),
        Mockito.anyLong(), Mockito.anyString());
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).completeFile(FILE_ID);
    Mockito.verify(mBlockStoreContext, Mockito.timeout(1)).acquireWorkerClient();
    Mockito.verify(mBlockStoreContext, Mockito.timeout(1)).releaseWorkerClient(mWorkerClient);
  }

  /**
   * Tests that cancel() will cancel and close the underlying out streams, and delete from the under
   * file system. Also makes sure that cancel() doesn't persist or complete the file.
   */
  @Test
  public void cancelTest() throws Exception {
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.cancel();
    for (long streamIndex = 0; streamIndex < 2; streamIndex ++) {
      Assert.assertTrue(mTachyonOutStreamMap.get(streamIndex).isClosed());
      Assert.assertTrue(mTachyonOutStreamMap.get(streamIndex).isCanceled());
    }
    // Don't persist or complete the file if the stream was canceled
    Mockito.verify(mWorkerClient, Mockito.times(0)).persistFile(Mockito.anyLong(),
        Mockito.anyLong(), Mockito.anyString());
    Mockito.verify(mFileSystemMasterClient, Mockito.times(0)).completeFile(FILE_ID);

    Mockito.verify(mUnderFileSystem, Mockito.times(1)).delete(Mockito.anyString(),
        Mockito.eq(false));
  }

  /**
   * Tests that flush() will flush the under store stream.
   */
  @Test
  public void flushTest() throws IOException {
    Assert.assertFalse(mUnderStorageFlushed.get());
    mTestStream.flush();
    Assert.assertTrue(mUnderStorageFlushed.get());
  }

  /**
   * Tests that if an exception is thrown by the underlying out stream, and the user is using
   * NO_PERSIST for their under storage type, the correct exception message will be thrown.
   */
  @Test
  public void cacheWriteExceptionNonSyncPersistTest() throws IOException {
    OutStreamOptions options = new OutStreamOptions.Builder(ClientContext.getConf())
        .setBlockSizeBytes(BLOCK_LENGTH).setUnderStorageType(UnderStorageType.NO_PERSIST).build();
    mTestStream = createTestStream(FILE_ID, options);

    BufferedBlockOutStream stream = Mockito.mock(BufferedBlockOutStream.class);
    Whitebox.setInternalState(mTestStream, "mCurrentBlockOutStream", stream);
    Mockito.when(stream.remaining()).thenReturn(BLOCK_LENGTH);
    Mockito.doThrow(new IOException("test error")).when(stream).write((byte) 7);
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.FAILED_CACHE.getMessage("test error"));
    mTestStream.write(7);
  }

  /**
   * Tests that if an exception is thrown by the underlying out stream, and the user is using
   * SYNC_PERSIST for their under storage type, the error is recovered from by writing the data to
   * the under storage out stream and setting the current block as not cacheable.
   */
  @Test
  public void cacheWriteExceptionSyncPersistTest() throws IOException {
    BufferedBlockOutStream stream = Mockito.mock(BufferedBlockOutStream.class);
    Whitebox.setInternalState(mTestStream, "mCurrentBlockOutStream", stream);
    Mockito.when(stream.remaining()).thenReturn(BLOCK_LENGTH);
    Mockito.doThrow(new IOException("test error")).when(stream).write((byte) 7);
    mTestStream.write(7);
    Assert.assertArrayEquals(new byte[]{7}, mUnderStorageOutputStream.toByteArray());
    Assert
        .assertFalse((Boolean) Whitebox.getInternalState(mTestStream, "mShouldCacheCurrentBlock"));
  }

  /**
   * Tests that write only writes a byte.
   */
  @Test
  public void truncateWriteTest() throws IOException {
    // Only writes the lowest byte
    mTestStream.write(0x1fffff00);
    mTestStream.write(0x1fffff01);
    verifyIncreasingBytesWritten(2);
  }

  /**
   * Tests that the correct exception is thrown when a buffer is written with invalid offset/length.
   */
  @Test
  public void writeBadBufferOffsetTest() throws IOException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_BUFFER_STATE, 10, 5, 6));
    mTestStream.write(new byte[10], 5, 6);
  }

  /**
   * Tests that writing a null buffer throws the correct exception.
   */
  @Test
  public void writeNullBufferTest() throws IOException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    mTestStream.write(null);
  }

  /**
   * Tests that writing a null buffer with offset/length information throws the correct exception.
   */
  @Test
  public void writeNullBufferOffsetTest() throws IOException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    mTestStream.write(null, 0, 0);
  }

  private void verifyIncreasingBytesWritten(int len) {
    verifyIncreasingBytesWritten(0, len);
  }

  /**
   * Verifies that the out streams have had exactly `len` increasing bytes written to them, with the
   * first byte starting at `start`. Also verifies that the same bytes have been written to the
   * under storage file stream.
   */
  private void verifyIncreasingBytesWritten(int start, int len) {
    long filledStreams = len / BLOCK_LENGTH;
    for (long streamIndex = 0; streamIndex < filledStreams; streamIndex ++) {
      Assert.assertTrue("stream " + streamIndex + " was never written",
          mTachyonOutStreamMap.containsKey(streamIndex));
      Assert.assertArrayEquals(BufferUtils
          .getIncreasingByteArray((int) (streamIndex * BLOCK_LENGTH + start), (int) BLOCK_LENGTH),
          mTachyonOutStreamMap.get(streamIndex).getDataWritten());
    }
    long lastStreamBytes = len - filledStreams * BLOCK_LENGTH;
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) (filledStreams * BLOCK_LENGTH + start),
            (int) lastStreamBytes),
        mTachyonOutStreamMap.get(filledStreams).getDataWritten());

    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(start, len),
        mUnderStorageOutputStream.toByteArray());
  }

  private FileOutStream createTestStream(long fileId, OutStreamOptions options) throws IOException {
    Whitebox.setInternalState(BlockStoreContext.class, "INSTANCE", mBlockStoreContext);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", mFileSystemContext);
    FileOutStream stream = new FileOutStream(FILE_ID, options);
    return stream;
  }
}
