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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyByte;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.BufferedBlockOutStream;
import alluxio.client.block.TestBufferedBlockOutStream;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.CreateUfsFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.util.ClientMockUtils;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.security.authorization.Permission;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for the {@link FileOutStream} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class, AlluxioBlockStore.class,
    UnderFileSystem.class})
public class FileOutStreamTest {

  private static final long BLOCK_LENGTH = 100L;
  private static final AlluxioURI FILE_NAME = new AlluxioURI("/file");
  /** Used if ufs operation delegation is enabled. */
  private static final long UFS_FILE_ID = 1L;

  private FileSystemContext mFileSystemContext;
  private AlluxioBlockStore mBlockStore;
  private FileSystemMasterClient mFileSystemMasterClient;
  private FileSystemWorkerClient mWorkerClient;
  private UnderFileSystem mUnderFileSystem;

  private Map<Long, TestBufferedBlockOutStream> mAlluxioOutStreamMap;
  private ByteArrayOutputStream mUnderStorageOutputStream;
  private AtomicBoolean mUnderStorageFlushed;

  private FileOutStream mTestStream;

  /**
   * Sets up the different contexts and clients before a test runs.
   */
  @Before
  public void before() throws Exception {
    ClientTestUtils.setSmallBufferSizes();

    // PowerMock enums and final classes
    mFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);

    when(mFileSystemContext.getAlluxioBlockStore()).thenReturn(mBlockStore);
    when(mFileSystemContext.acquireMasterClient()).thenReturn(mFileSystemMasterClient);
    when(mFileSystemMasterClient.getStatus(any(AlluxioURI.class))).thenReturn(
        new URIStatus(new FileInfo()));

    // Worker file client mocking
    mWorkerClient = PowerMockito.mock(FileSystemWorkerClient.class);
    when(mFileSystemContext.createWorkerClient()).thenReturn(mWorkerClient);
    when(mWorkerClient.createUfsFile(any(AlluxioURI.class), any(CreateUfsFileOptions.class)))
        .thenReturn(UFS_FILE_ID);

    // Return sequentially increasing numbers for new block ids
    when(mFileSystemMasterClient.getNewBlockIdForFile(FILE_NAME))
        .thenAnswer(new Answer<Long>() {
          private long mCount = 0;

          @Override
          public Long answer(InvocationOnMock invocation) throws Throwable {
            return mCount++;
          }
        });

    // Set up out streams. When they are created, add them to outStreamMap
    final Map<Long, TestBufferedBlockOutStream> outStreamMap = new HashMap<>();
    when(mBlockStore.getOutStream(anyLong(), eq(BLOCK_LENGTH),
        any(WorkerNetAddress.class))).thenAnswer(new Answer<BufferedBlockOutStream>() {
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
    BlockWorkerInfo workerInfo =
        new BlockWorkerInfo(new WorkerNetAddress().setHost("localhost").setRpcPort(1)
            .setDataPort(2).setWebPort(3), Constants.GB, 0);
    when(mBlockStore.getWorkerInfoList()).thenReturn(Lists.newArrayList(workerInfo));
    mAlluxioOutStreamMap = outStreamMap;

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
    when(mUnderFileSystem.create(anyString())).thenReturn(mUnderStorageOutputStream);
    when(mUnderFileSystem.create(anyString(), any(CreateOptions.class)))
        .thenReturn(mUnderStorageOutputStream);

    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.CACHE_THROUGH).setPermission(Permission.defaults());
    mTestStream = createTestStream(FILE_NAME, options);
  }

  @After
  public void after() {
    ClientTestUtils.resetClient();
  }

  /**
   * Tests that a single byte is written to the out stream correctly.
   */
  @Test
  public void singleByteWriteTest() throws Exception {
    mTestStream.write(5);
    Assert.assertArrayEquals(new byte[] {5}, mAlluxioOutStreamMap.get(0L).getWrittenData());
  }

  /**
   * Tests that many bytes, written one at a time, are written to the out streams correctly.
   */
  @Test
  public void manyBytesWriteTest() throws IOException {
    int bytesToWrite = (int) ((BLOCK_LENGTH * 5) + (BLOCK_LENGTH / 2));
    for (int i = 0; i < bytesToWrite; i++) {
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
   * Tests that {@link FileOutStream#close()} will close but not cancel the underlying out streams.
   * Also checks that {@link FileOutStream#close()} persists and completes the file.
   */
  @Test
  public void closeTest() throws Exception {
    when(mUnderFileSystem.rename(anyString(), anyString())).thenReturn(true);
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.close();
    for (long streamIndex = 0; streamIndex < 2; streamIndex++) {
      Assert.assertFalse(mAlluxioOutStreamMap.get(streamIndex).isCanceled());
      Assert.assertTrue(mAlluxioOutStreamMap.get(streamIndex).isClosed());
    }
    verify(mFileSystemMasterClient).completeFile(eq(FILE_NAME), any(CompleteFileOptions.class));
  }

  /**
   * Tests that {@link FileOutStream#cancel()} will cancel and close the underlying out streams,
   * and delete from the under file system. Also makes sure that cancel() doesn't persist or
   * complete the file.
   */
  @Test
  public void cancelTest() throws Exception {
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.cancel();
    for (long streamIndex = 0; streamIndex < 2; streamIndex++) {
      Assert.assertTrue(mAlluxioOutStreamMap.get(streamIndex).isClosed());
      Assert.assertTrue(mAlluxioOutStreamMap.get(streamIndex).isCanceled());
    }
    // Don't persist or complete the file if the stream was canceled
    verify(mFileSystemMasterClient, times(0)).completeFile(FILE_NAME,
        CompleteFileOptions.defaults());
    verify(mUnderFileSystem).delete(anyString(), eq(false));
  }

  /**
   * Tests that {@link FileOutStream#flush()} will flush the under store stream.
   */
  @Test
  public void flushTest() throws IOException {
    Assert.assertFalse(mUnderStorageFlushed.get());
    mTestStream.flush();
    Assert.assertTrue(mUnderStorageFlushed.get());
  }

  /**
   * Tests that if an exception is thrown by the underlying out stream, and the user is using
   * {@link UnderStorageType#NO_PERSIST} for their under storage type, the correct exception
   * message will be thrown.
   */
  @Test
  public void cacheWriteExceptionNonSyncPersistTest() throws IOException {
    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE);
    BufferedBlockOutStream stream = mock(BufferedBlockOutStream.class);
    when(mBlockStore.getOutStream(anyInt(), anyLong(), any(WorkerNetAddress.class)))
        .thenReturn(stream);
    mTestStream = createTestStream(FILE_NAME, options);

    when(stream.remaining()).thenReturn(BLOCK_LENGTH);
    doThrow(new IOException("test error")).when(stream).write((byte) 7);
    try {
      mTestStream.write(7);
      Assert.fail("the test should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.FAILED_CACHE.getMessage("test error"), e.getMessage());
    }
  }

  /**
   * Tests that if an exception is thrown by the underlying out stream, and the user is using
   * {@link UnderStorageType#SYNC_PERSIST} for their under storage type, the error is recovered
   * from by writing the data to the under storage out stream.
   */
  @Test
  public void cacheWriteExceptionSyncPersistTest() throws IOException {
    BufferedBlockOutStream stream = mock(BufferedBlockOutStream.class);
    when(mBlockStore.getOutStream(anyLong(), anyLong(), any(WorkerNetAddress.class)))
        .thenReturn(stream);

    when(stream.remaining()).thenReturn(BLOCK_LENGTH);
    doThrow(new IOException("test error")).when(stream).write((byte) 7);
    mTestStream.write(7);
    mTestStream.write(8);
    Assert.assertArrayEquals(new byte[] {7, 8}, mUnderStorageOutputStream.toByteArray());
    // The cache stream is written to only once - the FileInStream gives up on it after it throws
    // the first exception.
    verify(stream, times(1)).write(anyByte());
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
    try {
      mTestStream.write(new byte[10], 5, 6);
      Assert.fail("buffer write with invalid offset/length should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_BUFFER_STATE.toString(), 10, 5, 6),
          e.getMessage());
    }
  }

  /**
   * Tests that writing a null buffer throws the correct exception.
   */
  @Test
  public void writeNullBufferTest() throws IOException {
    try {
      mTestStream.write(null);
      Assert.fail("writing null should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(PreconditionMessage.ERR_WRITE_BUFFER_NULL.toString(), e.getMessage());
    }
  }

  /**
   * Tests that writing a null buffer with offset/length information throws the correct exception.
   */
  @Test
  public void writeNullBufferOffsetTest() throws IOException {
    try {
      mTestStream.write(null, 0, 0);
      Assert.fail("writing null should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(PreconditionMessage.ERR_WRITE_BUFFER_NULL.toString(), e.getMessage());
    }
  }

  /**
   * Tests that the async write invokes the expected client APIs.
   */
  @Test
  public void asyncWriteTest() throws Exception {
    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.ASYNC_THROUGH);
    mTestStream = createTestStream(FILE_NAME, options);

    when(mUnderFileSystem.rename(anyString(), anyString())).thenReturn(true);
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.close();
    verify(mFileSystemMasterClient).completeFile(eq(FILE_NAME), any(CompleteFileOptions.class));
    verify(mFileSystemMasterClient).scheduleAsyncPersist(eq(FILE_NAME));
  }

  @Test
  public void useLocationPolicyTest() throws IOException {
    OutStreamOptions options = OutStreamOptions.defaults().setWriteType(WriteType.MUST_CACHE)
        .setLocationPolicy(new FileWriteLocationPolicy() {
          @Override
          public WorkerNetAddress getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList,
              long blockSizeBytes) {
            throw new RuntimeException("policy threw exception");
          }
        });
    mTestStream = createTestStream(FILE_NAME, options);
    try {
      mTestStream.write(5);
      Assert.fail("An exception should have been thrown");
    } catch (Exception e) {
      Assert.assertEquals("policy threw exception", e.getMessage());
    }
  }

  /**
   * Tests that the correct exception message is produced when the location policy is not specified.
   */
  @Test
  public void missingLocationPolicyTest() throws IOException {
    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE).setLocationPolicy(null);
    try {
      mTestStream = createTestStream(FILE_NAME, options);
      Assert.fail("missing location policy should fail");
    } catch (NullPointerException e) {
      Assert.assertEquals(PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED.toString(),
          e.getMessage());
    }
  }

  /**
   * Tests that the number of bytes written is correct when the stream is created with different
   * under storage types.
   */
  @Test
  public void getBytesWrittenWithDifferentUnderStorageTypeTest() throws IOException {
    for (WriteType type : WriteType.values()) {
      OutStreamOptions options =
          OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH).setWriteType(type);
      mTestStream = createTestStream(FILE_NAME, options);
      mTestStream.write(BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH));
      mTestStream.flush();
      Assert.assertEquals(BLOCK_LENGTH, mTestStream.getBytesWritten());
    }
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
    for (long streamIndex = 0; streamIndex < filledStreams; streamIndex++) {
      Assert.assertTrue("stream " + streamIndex + " was never written",
          mAlluxioOutStreamMap.containsKey(streamIndex));
      Assert.assertArrayEquals(BufferUtils
          .getIncreasingByteArray((int) (streamIndex * BLOCK_LENGTH + start), (int) BLOCK_LENGTH),
          mAlluxioOutStreamMap.get(streamIndex).getWrittenData());
    }
    long lastStreamBytes = len - filledStreams * BLOCK_LENGTH;
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) (filledStreams * BLOCK_LENGTH + start),
            (int) lastStreamBytes),
        mAlluxioOutStreamMap.get(filledStreams).getWrittenData());

    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(start, len),
        mUnderStorageOutputStream.toByteArray());
  }

  private FileOutStream createTestStream(AlluxioURI path, OutStreamOptions options)
      throws IOException {
    FileOutStream stream = new FileOutStream(path, options, mFileSystemContext,
        UnderFileSystemFileOutStream.Factory.get());
    return stream;
  }
}
