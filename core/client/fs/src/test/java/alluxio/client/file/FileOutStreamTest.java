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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.block.stream.TestBlockOutStream;
import alluxio.client.block.stream.TestUnderFileSystemFileOutStream;
import alluxio.client.block.stream.UnderFileSystemFileOutStream;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.util.ClientTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.WritePType;
import alluxio.network.TieredIdentityFactory;
import alluxio.resource.DummyCloseableResource;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for the {@link FileOutStream} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class, AlluxioBlockStore.class,
    UnderFileSystemFileOutStream.class})
public class FileOutStreamTest {

  private static InstancedConfiguration sConf = ConfigurationTestUtils.defaults();

  private static final long BLOCK_LENGTH = 100L;
  private static final AlluxioURI FILE_NAME = new AlluxioURI("/file");

  private FileSystemContext mFileSystemContext;
  private AlluxioBlockStore mBlockStore;
  private FileSystemMasterClient mFileSystemMasterClient;

  private Map<Long, TestBlockOutStream> mAlluxioOutStreamMap;
  private TestUnderFileSystemFileOutStream mUnderStorageOutputStream;
  private AtomicBoolean mUnderStorageFlushed;

  private AlluxioFileOutStream mTestStream;
  private ClientContext mClientContext;

  /**
   * Sets up the different contexts and clients before a test runs.
   */
  @Before
  public void before() throws Exception {
    GroupMappingServiceTestUtils.resetCache();
    ClientTestUtils.setSmallBufferSizes(sConf);

    mClientContext = ClientContext.create(sConf);

    // PowerMock enums and final classes
    mFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    when(mFileSystemContext.getClientContext()).thenReturn(mClientContext);
    when(mFileSystemContext.getClusterConf()).thenReturn(sConf);
    when(mFileSystemContext.getPathConf(any(AlluxioURI.class))).thenReturn(sConf);
    mBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);

    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mFileSystemContext)).thenReturn(mBlockStore);

    when(mFileSystemContext.acquireMasterClientResource())
        .thenReturn(new DummyCloseableResource<>(mFileSystemMasterClient));
    when(mFileSystemMasterClient.getStatus(any(AlluxioURI.class), any(GetStatusPOptions.class)))
        .thenReturn(new URIStatus(new FileInfo()));

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
    final Map<Long, TestBlockOutStream> outStreamMap = new HashMap<>();
    when(mBlockStore.getOutStream(anyLong(), eq(BLOCK_LENGTH),
        any(OutStreamOptions.class))).thenAnswer(new Answer<TestBlockOutStream>() {
          @Override
          public TestBlockOutStream answer(InvocationOnMock invocation) throws Throwable {
            Long blockId = invocation.getArgument(0, Long.class);
            if (!outStreamMap.containsKey(blockId)) {
              TestBlockOutStream newStream =
                  new TestBlockOutStream(ByteBuffer.allocate(1000), BLOCK_LENGTH);
              outStreamMap.put(blockId, newStream);
            }
            return outStreamMap.get(blockId);
          }
        });
    BlockWorkerInfo workerInfo =
        new BlockWorkerInfo(new WorkerNetAddress().setHost("localhost")
            .setTieredIdentity(TieredIdentityFactory.fromString("node=localhost", sConf))
            .setRpcPort(1).setDataPort(2).setWebPort(3), Constants.GB, 0);
    when(mFileSystemContext.getCachedWorkers()).thenReturn(Lists.newArrayList(workerInfo));
    mAlluxioOutStreamMap = outStreamMap;

    // Create an under storage stream so that we can check whether it has been flushed
    final AtomicBoolean underStorageFlushed = new AtomicBoolean(false);
    mUnderStorageOutputStream =
        new TestUnderFileSystemFileOutStream(ByteBuffer.allocate(5000)) {
          @Override
          public void flush() throws IOException {
            super.flush();
            underStorageFlushed.set(true);
          }
        };
    mUnderStorageFlushed = underStorageFlushed;

    PowerMockito.mockStatic(UnderFileSystemFileOutStream.class);
    PowerMockito.when(
        UnderFileSystemFileOutStream.create(any(FileSystemContext.class),
            any(WorkerNetAddress.class), any(OutStreamOptions.class))).thenReturn(
        mUnderStorageOutputStream);

    OutStreamOptions options =
        OutStreamOptions.defaults(mClientContext).setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.CACHE_THROUGH).setUfsPath(FILE_NAME.getPath());
    mTestStream = createTestStream(FILE_NAME, options);
  }

  @After
  public void after() {
    ClientTestUtils.resetClient(sConf);
  }

  /**
   * Tests that a single byte is written to the out stream correctly.
   */
  @Test
  public void singleByteWrite() throws Exception {
    mTestStream.write(5);
    mTestStream.close();
    assertArrayEquals(new byte[] {5}, mAlluxioOutStreamMap.get(0L).getWrittenData());
  }

  /**
   * Tests that many bytes, written one at a time, are written to the out streams correctly.
   */
  @Test
  public void manyBytesWrite() throws IOException {
    int bytesToWrite = (int) ((BLOCK_LENGTH * 5) + (BLOCK_LENGTH / 2));
    for (int i = 0; i < bytesToWrite; i++) {
      mTestStream.write(i);
    }
    mTestStream.close();
    verifyIncreasingBytesWritten(bytesToWrite);
  }

  /**
   * Tests that writing a buffer all at once will write bytes to the out streams correctly.
   */
  @Test
  public void writeBuffer() throws IOException {
    int bytesToWrite = (int) ((BLOCK_LENGTH * 5) + (BLOCK_LENGTH / 2));
    mTestStream.write(BufferUtils.getIncreasingByteArray(bytesToWrite));
    mTestStream.close();
    verifyIncreasingBytesWritten(bytesToWrite);
  }

  /**
   * Tests writing a buffer at an offset.
   */
  @Test
  public void writeOffset() throws IOException {
    int bytesToWrite = (int) ((BLOCK_LENGTH * 5) + (BLOCK_LENGTH / 2));
    int offset = (int) (BLOCK_LENGTH / 3);
    mTestStream.write(BufferUtils.getIncreasingByteArray(bytesToWrite + offset), offset,
        bytesToWrite);
    mTestStream.close();
    verifyIncreasingBytesWritten(offset, bytesToWrite);
  }

  /**
   * Tests that {@link FileOutStream#close()} will close but not cancel the underlying out streams.
   * Also checks that {@link FileOutStream#close()} persists and completes the file.
   */
  @Test
  public void close() throws Exception {
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.close();
    for (long streamIndex = 0; streamIndex < 2; streamIndex++) {
      assertFalse(mAlluxioOutStreamMap.get(streamIndex).isCanceled());
      assertTrue(mAlluxioOutStreamMap.get(streamIndex).isClosed());
    }
    verify(mFileSystemMasterClient).completeFile(eq(FILE_NAME), any(CompleteFilePOptions.class));
  }

  /**
   * Tests that {@link FileOutStream#cancel()} will cancel and close the underlying out streams, and
   * delete from the under file system when the delegation flag is set. Also makes sure that
   * cancel() doesn't persist or complete the file.
   */
  @Test
  public void cancelWithDelegation() throws Exception {
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.cancel();
    for (long streamIndex = 0; streamIndex < 2; streamIndex++) {
      assertTrue(mAlluxioOutStreamMap.get(streamIndex).isClosed());
      assertTrue(mAlluxioOutStreamMap.get(streamIndex).isCanceled());
    }
    // Don't complete the file if the stream was canceled
    verify(mFileSystemMasterClient, times(0)).completeFile(any(AlluxioURI.class),
        any(CompleteFilePOptions.class));
  }

  /**
   * Tests that {@link FileOutStream#flush()} will flush the under store stream.
   */
  @Test
  public void flush() throws IOException {
    assertFalse(mUnderStorageFlushed.get());
    mTestStream.flush();
    assertTrue(mUnderStorageFlushed.get());
  }

  /**
   * Tests that if an exception is thrown by the underlying out stream, and the user is using
   * {@link UnderStorageType#NO_PERSIST} for their under storage type, the correct exception
   * message will be thrown.
   */
  @Test
  public void cacheWriteExceptionNonSyncPersist() throws IOException {
    OutStreamOptions options =
        OutStreamOptions.defaults(mClientContext).setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE);
    BlockOutStream stream = mock(BlockOutStream.class);
    when(mBlockStore.getOutStream(anyLong(), anyLong(), any(OutStreamOptions.class)))
        .thenReturn(stream);
    mTestStream = createTestStream(FILE_NAME, options);

    when(stream.remaining()).thenReturn(BLOCK_LENGTH);
    doThrow(new IOException("test error")).when(stream).write(7);

    try {
      mTestStream.write(7);
      fail("the test should fail");
    } catch (IOException e) {
      assertEquals(ExceptionMessage.FAILED_CACHE.getMessage("test error"), e.getMessage());
    }
  }

  /**
   * Tests that if an exception is thrown by the underlying out stream, and the user is using
   * {@link UnderStorageType#SYNC_PERSIST} for their under storage type, the error is recovered
   * from by writing the data to the under storage out stream.
   */
  @Test
  public void cacheWriteExceptionSyncPersist() throws IOException {
    BlockOutStream stream = mock(BlockOutStream.class);
    when(mBlockStore.getOutStream(anyLong(), anyLong(), any(OutStreamOptions.class)))
        .thenReturn(stream);

    when(stream.remaining()).thenReturn(BLOCK_LENGTH);
    doThrow(new IOException("test error")).when(stream).write((byte) 7);
    mTestStream.write(7);
    mTestStream.write(8);
    assertArrayEquals(new byte[] {7, 8}, mUnderStorageOutputStream.getWrittenData());
    // The cache stream is written to only once - the FileInStream gives up on it after it throws
    // the first exception.
    verify(stream, times(1)).write(anyInt());
  }

  /**
   * Tests that write only writes a byte.
   */
  @Test
  public void truncateWrite() throws IOException {
    // Only writes the lowest byte
    mTestStream.write(0x1fffff00);
    mTestStream.write(0x1fffff01);
    mTestStream.close();
    verifyIncreasingBytesWritten(2);
  }

  /**
   * Tests that the correct exception is thrown when a buffer is written with invalid offset/length.
   */
  @Test
  public void writeBadBufferOffset() throws IOException {
    try {
      mTestStream.write(new byte[10], 5, 6);
      fail("buffer write with invalid offset/length should fail");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(PreconditionMessage.ERR_BUFFER_STATE.toString(), 10, 5, 6),
          e.getMessage());
    }
  }

  /**
   * Tests that writing a null buffer throws the correct exception.
   */
  @Test
  public void writeNullBuffer() throws IOException {
    try {
      mTestStream.write(null);
      fail("writing null should fail");
    } catch (IllegalArgumentException e) {
      assertEquals(PreconditionMessage.ERR_WRITE_BUFFER_NULL.toString(), e.getMessage());
    }
  }

  /**
   * Tests that writing a null buffer with offset/length information throws the correct exception.
   */
  @Test
  public void writeNullBufferOffset() throws IOException {
    try {
      mTestStream.write(null, 0, 0);
      fail("writing null should fail");
    } catch (IllegalArgumentException e) {
      assertEquals(PreconditionMessage.ERR_WRITE_BUFFER_NULL.toString(), e.getMessage());
    }
  }

  /**
   * Tests that the async write invokes the expected client APIs.
   */
  @Test
  public void asyncWrite() throws Exception {
    OutStreamOptions options =
        OutStreamOptions.defaults(mClientContext).setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.ASYNC_THROUGH);
    mTestStream = createTestStream(FILE_NAME, options);

    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.close();

    // Verify that async persist request is sent with complete file request.
    ArgumentCaptor<CompleteFilePOptions> parameterCaptor =
            ArgumentCaptor.forClass(CompleteFilePOptions.class);
    verify(mFileSystemMasterClient).completeFile(eq(FILE_NAME), parameterCaptor.capture());
    Assert.assertTrue(parameterCaptor.getValue().hasAsyncPersistOptions());
  }

  /**
   * Tests that common options are propagated to async write request.
   */
  @Test
  public void asyncWriteOptionPropagation() throws Exception {
    Random rand = new Random();
    FileSystemMasterCommonPOptions commonOptions =
        FileSystemMasterCommonPOptions.newBuilder().setTtl(rand.nextLong())
            .setTtlAction(TtlAction.values()[rand.nextInt(TtlAction.values().length)])
            .setSyncIntervalMs(rand.nextLong()).build();

    OutStreamOptions options =
        new OutStreamOptions(CreateFilePOptions.newBuilder().setWriteType(WritePType.ASYNC_THROUGH)
            .setBlockSizeBytes(BLOCK_LENGTH).setCommonOptions(commonOptions).build(),
            mClientContext, sConf);

    // Verify that OutStreamOptions have captured the common options properly.
    assertEquals(options.getCommonOptions(), commonOptions);

    mTestStream = createTestStream(FILE_NAME, options);
    mTestStream.write(BufferUtils.getIncreasingByteArray((int) (BLOCK_LENGTH * 1.5)));
    mTestStream.close();
    verify(mFileSystemMasterClient).completeFile(eq(FILE_NAME), any(CompleteFilePOptions.class));

    // Verify that common options for OutStreamOptions are propagated to ScheduleAsyncPersistence.
    ArgumentCaptor<CompleteFilePOptions> parameterCaptor =
            ArgumentCaptor.forClass(CompleteFilePOptions.class);

    verify(mFileSystemMasterClient).completeFile(eq(FILE_NAME), parameterCaptor.capture());
    assertEquals(parameterCaptor.getValue().getAsyncPersistOptions().getCommonOptions(),
        options.getCommonOptions());
  }

  /**
   * Tests that the number of bytes written is correct when the stream is created with different
   * under storage types.
   */
  @Test
  public void getBytesWrittenWithDifferentUnderStorageType() throws IOException {
    for (WriteType type : WriteType.values()) {
      OutStreamOptions options =
          OutStreamOptions.defaults(mClientContext).setBlockSizeBytes(BLOCK_LENGTH)
              .setWriteType(type).setUfsPath(FILE_NAME.getPath());
      mTestStream = createTestStream(FILE_NAME, options);
      mTestStream.write(BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH));
      mTestStream.flush();
      assertEquals(BLOCK_LENGTH, mTestStream.getBytesWritten());
    }
  }

  @Test
  public void createWithNoWorker() throws Exception {
    OutStreamOptions options = OutStreamOptions.defaults(mClientContext)
        .setLocationPolicy((getWorkerOptions) -> null)
        .setWriteType(WriteType.CACHE_THROUGH);
    Exception e = assertThrows(UnavailableException.class,
        () -> mTestStream = createTestStream(FILE_NAME, options));
    assertTrue(e.getMessage().contains(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage()));
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
      assertTrue("stream " + streamIndex + " was never written",
          mAlluxioOutStreamMap.containsKey(streamIndex));
      assertArrayEquals(BufferUtils
          .getIncreasingByteArray((int) (streamIndex * BLOCK_LENGTH + start), (int) BLOCK_LENGTH),
          mAlluxioOutStreamMap.get(streamIndex).getWrittenData());
    }
    long lastStreamBytes = len - filledStreams * BLOCK_LENGTH;
    assertArrayEquals(
        BufferUtils.getIncreasingByteArray((int) (filledStreams * BLOCK_LENGTH + start),
            (int) lastStreamBytes),
        mAlluxioOutStreamMap.get(filledStreams).getWrittenData());

    assertArrayEquals(BufferUtils.getIncreasingByteArray(start, len),
        mUnderStorageOutputStream.getWrittenData());
  }

  /**
   * Creates a {@link FileOutStream} for test.
   *
   * @param path the file path
   * @param options the set of options specific to this operation
   * @return a {@link FileOutStream}
   */
  private AlluxioFileOutStream createTestStream(AlluxioURI path, OutStreamOptions options)
      throws IOException {
    return new AlluxioFileOutStream(path, options, mFileSystemContext);
  }
}
