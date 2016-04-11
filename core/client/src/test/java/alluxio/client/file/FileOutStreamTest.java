/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.BufferedBlockOutStream;
import alluxio.client.block.TestBufferedBlockOutStream;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.OutStreamOptions;
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
import com.google.common.collect.Maps;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for the {@link FileOutStream} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, BlockStoreContext.class, FileSystemMasterClient.class,
    AlluxioBlockStore.class, UnderFileSystem.class, BlockWorkerClient.class})
public class FileOutStreamTest {

  private static final long BLOCK_LENGTH = 100L;
  private static final AlluxioURI FILE_NAME = new AlluxioURI("/file");

  private AlluxioBlockStore mBlockStore;
  private BlockStoreContext mBlockStoreContext;
  private FileSystemContext mFileSystemContext;
  private FileSystemMasterClient mFileSystemMasterClient;
  private UnderFileSystem mUnderFileSystem;

  private Map<Long, TestBufferedBlockOutStream> mAlluxioOutStreamMap;
  private ByteArrayOutputStream mUnderStorageOutputStream;
  private AtomicBoolean mUnderStorageFlushed;

  private FileOutStream mTestStream;

  /**
   * Sets up the different contexts and clients before a test runs.
   *
   * @throws Exception when the {@link FileSystemMasterClient} fails
   */
  @Before
  public void before() throws Exception {
    ClientTestUtils.setSmallBufferSizes();

    // PowerMock enums and final classes
    mFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mBlockStoreContext = PowerMockito.mock(BlockStoreContext.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);

    Mockito.when(mFileSystemContext.getAlluxioBlockStore()).thenReturn(mBlockStore);
    Mockito.when(mFileSystemContext.acquireMasterClient()).thenReturn(mFileSystemMasterClient);
    Mockito.when(mFileSystemMasterClient.getStatus(Mockito.any(AlluxioURI.class))).thenReturn(
        new URIStatus(new FileInfo()));

    // Return sequentially increasing numbers for new block ids
    Mockito.when(mFileSystemMasterClient.getNewBlockIdForFile(FILE_NAME))
        .thenAnswer(new Answer<Long>() {
          private long mCount = 0;

          @Override
          public Long answer(InvocationOnMock invocation) throws Throwable {
            return mCount++;
          }
        });

    // Set up out streams. When they are created, add them to outStreamMap
    final Map<Long, TestBufferedBlockOutStream> outStreamMap = Maps.newHashMap();
    Mockito.when(mBlockStore.getOutStream(Mockito.anyLong(), Mockito.eq(BLOCK_LENGTH),
        Mockito.any(WorkerNetAddress.class))).thenAnswer(new Answer<BufferedBlockOutStream>() {
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
    Mockito.when(mBlockStore.getWorkerInfoList()).thenReturn(Lists.newArrayList(workerInfo));
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
    Mockito.when(mUnderFileSystem.create(Mockito.anyString(), Mockito.eq((int) BLOCK_LENGTH)))
        .thenReturn(mUnderStorageOutputStream);

    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.CACHE_THROUGH);
    mTestStream = createTestStream(FILE_NAME, options);
  }

  @After
  public void after() {
    ClientTestUtils.resetClientContext();
  }

  /**
   * Tests that a single byte is written to the out stream correctly.
   *
   * @throws Exception when the write fails
   */
  @Test
  public void singleByteWriteTest() throws Exception {
    mTestStream.write(5);
    Assert.assertArrayEquals(new byte[] {5}, mAlluxioOutStreamMap.get(0L).getWrittenData());
  }

  /**
   * Tests that many bytes, written one at a time, are written to the out streams correctly.
   *
   * @throws IOException when the write fails
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
   *
   * @throws IOException when the write fails
   */
  @Test
  public void writeBufferTest() throws IOException {
    int bytesToWrite = (int) ((BLOCK_LENGTH * 5) + (BLOCK_LENGTH / 2));
    mTestStream.write(BufferUtils.getIncreasingByteArray(bytesToWrite));
    verifyIncreasingBytesWritten(bytesToWrite);
  }

  /**
   * Tests writing a buffer at an offset.
   *
   * @throws IOException when the write fails
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
   *
   * @throws Exception when the write fails
   */
  @Test
  public void closeTest() throws Exception {
    Mockito.when(mUnderFileSystem.rename(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(true);
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.close();
    for (long streamIndex = 0; streamIndex < 2; streamIndex++) {
      Assert.assertFalse(mAlluxioOutStreamMap.get(streamIndex).isCanceled());
      Assert.assertTrue(mAlluxioOutStreamMap.get(streamIndex).isClosed());
    }
    Mockito.verify(mFileSystemMasterClient).completeFile(Mockito.eq(FILE_NAME),
        Mockito.any(CompleteFileOptions.class));
  }

  /**
   * Tests that {@link FileOutStream#cancel()} will cancel and close the underlying out streams,
   * and delete from the under file system. Also makes sure that cancel() doesn't persist or
   * complete the file.
   *
   * @throws Exception when the write fails
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
    Mockito.verify(mFileSystemMasterClient, Mockito.times(0)).completeFile(FILE_NAME,
        CompleteFileOptions.defaults());

    Mockito.verify(mUnderFileSystem).delete(Mockito.anyString(), Mockito.eq(false));
  }

  /**
   * Tests that {@link FileOutStream#flush()} will flush the under store stream.
   *
   * @throws IOException when the flushing fails
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
   *
   * @throws  IOException when the write fails
   */
  @Test
  public void cacheWriteExceptionNonSyncPersistTest() throws IOException {
    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE);
    mTestStream = createTestStream(FILE_NAME, options);

    BufferedBlockOutStream stream = Mockito.mock(BufferedBlockOutStream.class);
    Whitebox.setInternalState(mTestStream, "mCurrentBlockOutStream", stream);
    Mockito.when(stream.remaining()).thenReturn(BLOCK_LENGTH);
    Mockito.doThrow(new IOException("test error")).when(stream).write((byte) 7);
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
   * from by writing the data to the under storage out stream and setting the current block as not
   * cacheable.
   *
   * @throws IOException when the write fails
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
   *
   * @throws IOException when the write fails
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
   *
   * @throws IOException when the write fails
   */
  @Test
  public void writeBadBufferOffsetTest() throws IOException {
    try {
      mTestStream.write(new byte[10], 5, 6);
      Assert.fail("buffer write with invalid offset/length should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_BUFFER_STATE, 10, 5, 6),
          e.getMessage());
    }
  }

  /**
   * Tests that writing a null buffer throws the correct exception.
   *
   * @throws IOException when the write fails
   */
  @Test
  public void writeNullBufferTest() throws IOException {
    try {
      mTestStream.write(null);
      Assert.fail("writing null should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(PreconditionMessage.ERR_WRITE_BUFFER_NULL, e.getMessage());
    }
  }

  /**
   * Tests that writing a null buffer with offset/length information throws the correct exception.
   *
   * @throws IOException when the write fails
   */
  @Test
  public void writeNullBufferOffsetTest() throws IOException {
    try {
      mTestStream.write(null, 0, 0);
      Assert.fail("writing null should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(PreconditionMessage.ERR_WRITE_BUFFER_NULL, e.getMessage());
    }
  }

  /**
   * Tests that the async write invokes the expected client APIs.
   *
   * @throws Exception when the write fails
   */
  @Test
  public void asyncWriteTest() throws Exception {
    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.ASYNC_THROUGH);
    mTestStream = createTestStream(FILE_NAME, options);

    Mockito.when(mUnderFileSystem.rename(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(true);
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.close();
    Mockito.verify(mFileSystemMasterClient).completeFile(Mockito.eq(FILE_NAME),
        Mockito.any(CompleteFileOptions.class));
    Mockito.verify(mFileSystemMasterClient).scheduleAsyncPersist(Mockito.eq(FILE_NAME));
  }

  /**
   * Tests the location policy created with different options.
   *
   * @throws IOException if creating the the test stream fails
   */
  @Test
  public void locationPolicyTest() throws IOException {
    OutStreamOptions options =
        OutStreamOptions.defaults().setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE);
    mTestStream = createTestStream(FILE_NAME, options);

    // by default local first policy used
    FileWriteLocationPolicy policy = Whitebox.getInternalState(mTestStream, "mLocationPolicy");
    Assert.assertTrue(policy instanceof LocalFirstPolicy);

    // configure a different policy
    options.setLocationPolicy(new RoundRobinPolicy());
    mTestStream = createTestStream(FILE_NAME, options);
    policy = Whitebox.getInternalState(mTestStream, "mLocationPolicy");
    Assert.assertTrue(policy instanceof RoundRobinPolicy);
  }

  /**
   * Tests that the correct exception message is produced when the location policy is not specified.
   *
   * @throws IOException if creating the test stream fails
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
      Assert.assertEquals(PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED,
          e.getMessage());
    }
  }

  /**
   * Tests that the number of bytes written is correct when the stream is created with different
   * under storage types.
   *
   * @throws IOException if creating the test stream fails
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
    Whitebox.setInternalState(BlockStoreContext.class, "INSTANCE", mBlockStoreContext);
    Whitebox.setInternalState(FileSystemContext.class, "INSTANCE", mFileSystemContext);
    FileOutStream stream = new FileOutStream(path, options);
    return stream;
  }
}
