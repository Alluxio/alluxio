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

package alluxio.client.block.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.ClientContext;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.grpc.Chunk;
import alluxio.grpc.RequestType;
import alluxio.grpc.WriteRequest;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.protobuf.ByteString;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, WorkerNetAddress.class})
public class UfsFallbackLocalFileDataWriterTest {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataWriterTest.class);

  /**
   * A data writer implementation which will throw a ResourceExhaustedException on writes when the
   * given ByteBuffer is full.
   */
  public static class FixedCapacityTestDataWriter extends TestDataWriter {
    private final long mCapacity;
    private final ByteBuffer mBuffer;
    private boolean mIsLocalWorkerFull = false;
    private boolean mClosed = false;
    private boolean mCanceled = false;

    public FixedCapacityTestDataWriter(ByteBuffer buffer) {
      super(buffer);
      mCapacity = buffer.capacity();
      mBuffer = buffer;
    }

    @Override
    public void writeChunk(ByteBuf chunk) throws IOException {
      if (pos() + chunk.readableBytes() > mCapacity) {
        mIsLocalWorkerFull = true;
      }
      if (mIsLocalWorkerFull) {
        throw new ResourceExhaustedException("no more space!");
      }
      synchronized (mBuffer) {
        super.writeChunk(chunk);
      }
    }

    @Override
    public void close() {
      super.close();
      if (mClosed) {
        return;
      }
      mClosed = true;
    }

    @Override
    public void cancel() {
      super.cancel();
      if (mCanceled) {
        return;
      }
      mCanceled = true;
      mClosed = true;
    }

    public boolean isClosed() {
      return mClosed;
    }

    public boolean isCanceled() {
      return mCanceled;
    }
  }

  private static final int CHUNK_SIZE = 1024;
  private static final ExecutorService EXECUTOR =
      Executors.newFixedThreadPool(4, ThreadFactoryUtils.build("test-executor-%d", true));

  private static final Random RANDOM = new Random();
  private static final long BLOCK_ID = 1L;
  private static final long MOUNT_ID = 9L;

  private ByteBuffer mBuffer;
  private FixedCapacityTestDataWriter mLocalWriter;
  private ClientContext mClientContext;
  private FileSystemContext mContext;
  private WorkerNetAddress mAddress;
  private BlockWorkerClient mClient;
  private ClientCallStreamObserver<WriteRequest> mRequestObserver;

  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(PropertyKey.USER_STREAMING_WRITER_CHUNK_SIZE_BYTES,
          String.valueOf(CHUNK_SIZE), mConf);

  @Before
  public void before() throws Exception {
    mClientContext = ClientContext.create(mConf);
    mContext = PowerMockito.mock(FileSystemContext.class);
    mAddress = Mockito.mock(WorkerNetAddress.class);

    mClient = mock(BlockWorkerClient.class);
    mRequestObserver = mock(ClientCallStreamObserver.class);
    PowerMockito.when(mContext.getClientContext()).thenReturn(mClientContext);
    PowerMockito.when(mContext.getClusterConf()).thenReturn(mConf);
    PowerMockito.when(mContext.acquireBlockWorkerClient(mAddress)).thenReturn(
        new NoopClosableResource<>(mClient));
    PowerMockito.when(mClient.writeBlock(any(StreamObserver.class))).thenReturn(mRequestObserver);
    PowerMockito.when(mRequestObserver.isReady()).thenReturn(true);
  }

  @After
  public void after() throws Exception {
    mClient.close();
  }

  /**
   * Creates a {@link DataWriter}.
   *
   * @param blockSize the block length
   * @param workerCapacity the capacity of the local worker
   * @return the data writer instance
   */
  private DataWriter create(long blockSize, long workerCapacity) throws Exception {
    mBuffer = ByteBuffer.allocate((int) workerCapacity);
    mLocalWriter = new FixedCapacityTestDataWriter(mBuffer);
    DataWriter writer =
        new UfsFallbackLocalFileDataWriter(mLocalWriter, null, mContext, mAddress, BLOCK_ID,
            blockSize, OutStreamOptions.defaults(mClientContext).setMountId(MOUNT_ID));
    return writer;
  }

  @Test
  public void emptyBlock() throws Exception {
    try (DataWriter writer = create(1, 1)) {
      writer.flush();
      assertEquals(0, writer.pos());
    }
    assertEquals(0, mBuffer.position());
  }

  @Test(timeout = 1000 * 60)
  public void noFallback() throws Exception {
    Future<WriteSummary> expected;
    Future<WriteSummary> actualLocal;
    long blockSize = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    try (DataWriter writer = create(blockSize, blockSize)) {
      expected = writeData(writer, blockSize);
      actualLocal = getLocalWrite(mBuffer);
      expected.get();
    }
    assertEquals(expected.get().getBytes(), actualLocal.get().getBytes());
    assertEquals(expected.get().getChecksum(), actualLocal.get().getChecksum());
  }

  @Ignore("Flaky test")
  @Test(timeout = 1000 * 60)
  public void fallbackOnFirstChunk() throws Exception {
    Future<WriteSummary> expected;
    Future<WriteSummary> actualLocal;
    Future<WriteSummary> actualUfs;
    long blockSize = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    try (DataWriter writer = create(blockSize, 1)) {
      expected = writeData(writer, blockSize);
      actualLocal = getLocalWrite(mBuffer);
      actualUfs = getUfsWrite(mClient);
      expected.get();
    }
    assertEquals(blockSize, expected.get().getBytes());
    assertEquals(0, actualLocal.get().getBytes());
    assertEquals(blockSize, actualUfs.get().getBytes());
    assertEquals(expected.get().getBytes(), actualUfs.get().getBytes());
    assertEquals(expected.get().getChecksum(), actualUfs.get().getChecksum());
  }

  @Ignore("Flaky test")
  @Test(timeout = 1000 * 60)
  public void fallbackOnSecondChunk() throws Exception {
    Future<WriteSummary> expected;
    Future<WriteSummary> actualLocal;
    Future<WriteSummary> actualUfs;
    long blockSize = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    try (DataWriter writer = create(blockSize, CHUNK_SIZE)) {
      expected = writeData(writer, blockSize);
      actualLocal = getLocalWrite(mBuffer);
      actualUfs = getUfsWrite(mClient);
      expected.get();
    }
    assertEquals(blockSize, expected.get().getBytes());
    assertEquals(CHUNK_SIZE, actualLocal.get().getBytes());
    assertEquals(blockSize - CHUNK_SIZE, actualUfs.get().getBytes());
    assertEquals(expected.get().getChecksum(),
        actualLocal.get().getChecksum() + actualUfs.get().getChecksum());
  }

  @Test(timeout = 1000 * 60)
  public void fallbackOnLastChunk() throws Exception {
    Future<WriteSummary> expected;
    Future<WriteSummary> actualLocal;
    Future<WriteSummary> actualUfs;
    long blockSize = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    try (DataWriter writer = create(blockSize, CHUNK_SIZE * 1024)) {
      expected = writeData(writer, blockSize);
      expected.get();
      actualLocal = getLocalWrite(mBuffer);
      actualUfs = getUfsWrite(mClient);
    }
    assertEquals(blockSize, expected.get().getBytes());
    assertEquals(CHUNK_SIZE * 1024, actualLocal.get().getBytes());
    assertEquals(blockSize - CHUNK_SIZE * 1024, actualUfs.get().getBytes());
    assertEquals(expected.get().getChecksum(),
        actualLocal.get().getChecksum() + actualUfs.get().getChecksum());
  }

  @Test(timeout = 1000 * 60)
  @Ignore("Flaky test")
  public void flush() throws Exception {
    long blockSize = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    try (DataWriter writer = create(blockSize, CHUNK_SIZE)) {
      Future<WriteSummary> expected;
      expected = writeData(writer, CHUNK_SIZE);
      expected.get();
      writer.flush();
      assertEquals(CHUNK_SIZE, mBuffer.position());
    }
  }

  @Test(timeout = 1000 * 60)
  @Ignore("Flaky test")
  public void pos() throws Exception {
    long blockSize = CHUNK_SIZE * 2 + CHUNK_SIZE / 3;
    try (DataWriter writer = create(blockSize, CHUNK_SIZE)) {
      byte[] data = new byte[1];
      Future<WriteSummary> actualUfs = getUfsWrite(mClient);
      for (long pos = 0; pos < blockSize; pos++) {
        assertEquals(pos, writer.pos());
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        writer.writeChunk(buf);
      }
      actualUfs.get();
    }
  }

  class WriteSummary {
    private final long mBytes;
    private final long mChecksum;

    public WriteSummary(long bytes, long checksum) {
      mBytes = bytes;
      mChecksum = checksum;
    }

    public long getBytes() {
      return mBytes;
    }

    public long getChecksum() {
      return mChecksum;
    }
  }

  /**
   * Writes chunks via the given data writer and returns a checksum for a region of the data
   * written.
   *
   * @param length the length
   * @return the checksum
   */
  private Future<WriteSummary> writeData(final DataWriter writer, final long length)
      throws Exception {
    return EXECUTOR.submit(new Callable<WriteSummary>() {
      @Override
      public WriteSummary call() throws IOException {
        try {
          long checksum = 0;
          long remaining = length;
          while (remaining > 0) {
            int bytesToWrite = (int) Math.min(remaining, CHUNK_SIZE);
            byte[] data = new byte[bytesToWrite];
            RANDOM.nextBytes(data);
            ByteBuf buf = Unpooled.wrappedBuffer(data);
            try {
              writer.writeChunk(buf);
            } catch (Exception e) {
              fail(e.getMessage());
              throw e;
            }
            remaining -= bytesToWrite;
            // TODO(binfan): create a util method to calculate checksum from buffer
            for (int i = 0; i < data.length; i++) {
              checksum += BufferUtils.byteToInt(data[i]);
            }
          }
          return new WriteSummary(length, checksum);
        } catch (Throwable throwable) {
          LOG.error("Failed to write file.", throwable);
          fail("Failed to write file." + throwable.getMessage());
          throw throwable;
        }
      }
    });
  }

  /**
   * Verifies the chunks written. After receiving the last chunk, it will also send an EOF to
   * the channel.
   *
   * @return the checksum of the data read starting from checksumStart
   */
  private Future<WriteSummary> getUfsWrite(final BlockWorkerClient channel) {
    return EXECUTOR.submit(new Callable<WriteSummary>() {
      @Override
      public WriteSummary call() throws TimeoutException, InterruptedException {
        try {
          ArgumentCaptor<WriteRequest> requestCaptor = ArgumentCaptor.forClass(WriteRequest.class);
          verify(mRequestObserver, atLeastOnce()).onNext(requestCaptor.capture());
          ArgumentCaptor<StreamObserver> captor = ArgumentCaptor.forClass(StreamObserver.class);
          verify(mClient).writeBlock(captor.capture());
          captor.getValue().onCompleted();
          long checksum = 0;
          long pos = 0;
          long len = 0;
          for (WriteRequest writeRequest : requestCaptor.getAllValues()) {
            validateWriteRequest(writeRequest, pos);
            if (writeRequest.hasCommand()
                && writeRequest.getCommand().getCreateUfsBlockOptions().hasBytesInBlockStore()) {
              pos += writeRequest.getCommand().getCreateUfsBlockOptions().getBytesInBlockStore();
              continue;
            }
            if (writeRequest.hasChunk()) {
              Chunk chunk = writeRequest.getChunk();
              Assert.assertTrue(chunk.hasData());
              ByteString buf = chunk.getData();
              for (byte b : buf.toByteArray()) {
                checksum += BufferUtils.byteToInt(b);
                pos++;
                len++;
              }
            }
          }
          return new WriteSummary(len, checksum);
        } catch (Throwable throwable) {
          fail("Failed to verify write requests." + throwable.getMessage());
          throw throwable;
        }
      }
    });
  }

  private Future<WriteSummary> getLocalWrite(final ByteBuffer buffer) {
    return EXECUTOR.submit(new Callable<WriteSummary>() {
      @Override
      public WriteSummary call() throws TimeoutException, InterruptedException {
        long checksum = 0;
        long pos = 0;
        CommonUtils.waitFor("Writing to local completes", () -> mLocalWriter.isClosed());
        synchronized (buffer) {
          int len = buffer.position();
          while (pos < len) {
            checksum += BufferUtils.byteToInt(buffer.get((int) pos));
            pos++;
          }
        }
        return new WriteSummary(pos, checksum);
      }
    });
  }

  /**
   * Validates the read request sent.
   *
   * @param request the request
   * @param offset the offset
   */
  private void validateWriteRequest(WriteRequest request, long offset) {
    if (request.hasCommand()) {
      assertEquals(RequestType.UFS_FALLBACK_BLOCK, request.getCommand().getType());
      assertEquals(BLOCK_ID, request.getCommand().getId());
      assertEquals(offset, request.getCommand().getOffset());
      assertTrue(request.getCommand().hasCreateUfsBlockOptions());
      assertEquals(MOUNT_ID, request.getCommand().getCreateUfsBlockOptions().getMountId());
    } else {
      assertTrue(request.hasChunk());
    }
  }
}
