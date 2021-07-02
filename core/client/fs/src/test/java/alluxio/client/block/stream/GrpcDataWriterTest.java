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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.ClientContext;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.grpc.Chunk;
import alluxio.grpc.RequestType;
import alluxio.grpc.WriteRequest;
import alluxio.resource.CloseableResource;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.protobuf.ByteString;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, WorkerNetAddress.class})
public final class GrpcDataWriterTest {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataWriterTest.class);

  private static final int CHUNK_SIZE = 1024;
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4,
      ThreadFactoryUtils.build("test-executor-%d", true));

  private static final Random RANDOM = new Random();
  private static final long BLOCK_ID = 1L;
  private static final int TIER = 0;

  private FileSystemContext mContext;
  private WorkerNetAddress mAddress;
  private BlockWorkerClient mClient;
  private ClientCallStreamObserver<WriteRequest> mRequestObserver;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private ClientContext mClientContext;

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(PropertyKey.USER_STREAMING_WRITER_CHUNK_SIZE_BYTES,
          String.valueOf(CHUNK_SIZE), mConf);

  @Before
  public void before() throws Exception {
    mClientContext = ClientContext.create(mConf);

    mContext = PowerMockito.mock(FileSystemContext.class);
    mAddress = mock(WorkerNetAddress.class);

    mClient = mock(BlockWorkerClient.class);
    mRequestObserver = mock(ClientCallStreamObserver.class);
    when(mContext.acquireBlockWorkerClient(mAddress)).thenReturn(
        new NoopClosableResource<>(mClient));
    when(mContext.getClientContext()).thenReturn(mClientContext);
    when(mContext.getClusterConf()).thenReturn(mConf);
    when(mClient.writeBlock(any(StreamObserver.class))).thenReturn(mRequestObserver);
    when(mRequestObserver.isReady()).thenReturn(true);
  }

  @After
  public void after() throws Exception {
    mClient.close();
  }

  /**
   * Writes an empty file.
   */
  @Test(timeout = 1000 * 60)
  public void writeEmptyFile() throws Exception {
    long checksumActual;
    try (DataWriter writer = create(10)) {
      checksumActual = verifyWriteRequests(mClient, 0, 10);
    }
    assertEquals(0, checksumActual);
  }

  /**
   * Writes a file with file length matches what is given and verifies the checksum of the whole
   * file.
   */
  @Test(timeout = 1000 * 60)
  public void writeFullFile() throws Exception {
    long checksumActual;
    Future<Long> checksumExpected;
    long length = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    try (DataWriter writer = create(length)) {
      checksumExpected = writeFile(writer, length, 0, length - 1);
      checksumExpected.get();
      checksumActual = verifyWriteRequests(mClient, 0, length - 1);
    }
    assertEquals(checksumExpected.get().longValue(), checksumActual);
  }

  /**
   * Writes a file with file length matches what is given and verifies the checksum of the whole
   * file.
   */
  @Test(timeout = 1000 * 60)
  public void writeFileChecksumOfPartialFile() throws Exception {
    long checksumActual;
    Future<Long> checksumExpected;
    long length = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    try (DataWriter writer = create(length)) {
      checksumExpected = writeFile(writer, length, 10, length / 3);
      checksumExpected.get();
      checksumActual = verifyWriteRequests(mClient, 10, length / 3);
    }
    assertEquals(checksumExpected.get().longValue(), checksumActual);
  }

  /**
   * Writes a file with unknown length.
   */
  @Test(timeout = 1000 * 60)
  public void writeFileUnknownLength() throws Exception {
    long checksumActual;
    Future<Long> checksumExpected;
    long length = CHUNK_SIZE * 1024;
    try (DataWriter writer = create(Long.MAX_VALUE)) {
      checksumExpected = writeFile(writer, length, 10, length / 3);
      checksumExpected.get();
      checksumActual = verifyWriteRequests(mClient, 10, length / 3);
    }
    assertEquals(checksumExpected.get().longValue(), checksumActual);
  }

  /**
   * Writes lots of chunks.
   */
  @Test(timeout = 1000 * 60)
  public void writeFileManyChunks() throws Exception {
    long checksumActual;
    Future<Long> checksumExpected;
    long length = CHUNK_SIZE * 30000 + CHUNK_SIZE / 3;
    try (DataWriter writer = create(Long.MAX_VALUE)) {
      checksumExpected = writeFile(writer, length, 10, length / 3);
      checksumExpected.get();
      checksumActual = verifyWriteRequests(mClient, 10, length / 3);
    }
    assertEquals(checksumExpected.get().longValue(), checksumActual);
  }

  @Test
  public void closedBlockWorkerClientTest() throws IOException {
    CloseableResource<BlockWorkerClient> resource = Mockito.mock(CloseableResource.class);
    when(resource.get()).thenReturn(mClient);
    FileSystemContext context = PowerMockito.mock(FileSystemContext.class);
    when(context.acquireBlockWorkerClient(any(WorkerNetAddress.class))).thenReturn(resource);
    when(context.getClusterConf()).thenReturn(mConf);
    mConf.set(PropertyKey.USER_STREAMING_WRITER_CLOSE_TIMEOUT, "1");
    GrpcDataWriter writer = GrpcDataWriter.create(context, mAddress, BLOCK_ID, 0,
        RequestType.ALLUXIO_BLOCK,
        OutStreamOptions.defaults(mClientContext).setWriteTier(0));
    verify(resource, times(0)).close();
    verifyWriteRequests(mClient, 0, 0);
    writer.close();
    verify(resource, times(1)).close();
  }

  /**
   * Creates a {@link DataWriter}.
   *
   * @param length the length
   * @return the data writer instance
   */
  private DataWriter create(long length) throws Exception {
    DataWriter writer =
        GrpcDataWriter.create(mContext, mAddress, BLOCK_ID, length,
            RequestType.ALLUXIO_BLOCK,
            OutStreamOptions.defaults(mClientContext).setWriteTier(TIER));
    return writer;
  }

  /**
   * Writes chunks of data via the given data writer and returns a checksum for a region of the data
   * written.
   *
   * @param length the length
   * @param start the start position to calculate the checksum
   * @param end the end position to calculate the checksum
   * @return the checksum
   */
  private Future<Long> writeFile(final DataWriter writer, final long length,
      final long start, final long end) throws Exception {
    return EXECUTOR.submit(new Callable<Long>() {
      @Override
      public Long call() throws IOException {
        try {
          long checksum = 0;
          long pos = 0;

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

            for (int i = 0; i < data.length; i++) {
              if (pos >= start && pos <= end) {
                checksum += BufferUtils.byteToInt(data[i]);
              }
              pos++;
            }
          }
          return checksum;
        } catch (Throwable throwable) {
          LOG.error("Failed to write file.", throwable);
          fail();
          throw throwable;
        }
      }
    });
  }

  /**
   * Verifies the packets written. After receiving the last packet, it will also send an EOF to
   * the client.
   *
   * @param checksumStart the start position to calculate the checksum
   * @param checksumEnd the end position to calculate the checksum
   * @return the checksum of the data read starting from checksumStart
   */
  private long verifyWriteRequests(final BlockWorkerClient client, final long checksumStart,
      final long checksumEnd) {
    try {
      ArgumentCaptor<WriteRequest> requestCaptor = ArgumentCaptor.forClass(WriteRequest.class);
      verify(mRequestObserver, atLeastOnce()).onNext(requestCaptor.capture());
      ArgumentCaptor<StreamObserver> captor = ArgumentCaptor.forClass(StreamObserver.class);
      verify(mClient).writeBlock(captor.capture());
      captor.getValue().onCompleted();
      long checksum = 0;
      long pos = 0;
      for (WriteRequest request : requestCaptor.getAllValues()) {
        validateWriteRequest(request, pos);
        if (request.hasChunk()) {
          Chunk chunk = request.getChunk();
          assertTrue(chunk.hasData());
          ByteString buf = chunk.getData();
          for (byte b : buf.toByteArray()) {
            if (pos >= checksumStart && pos <= checksumEnd) {
              checksum += BufferUtils.byteToInt(b);
            }
            pos++;
          }
        }
      }
      return checksum;
    } catch (Throwable throwable) {
      LOG.error("Failed to verify write requests.", throwable);
      fail();
      throw throwable;
    }
  }

  /**
   * Validates the read request sent.
   *
   * @param request the request
   * @param offset the offset
   */
  private void validateWriteRequest(WriteRequest request, long offset) {
    if (request.hasCommand()) {
      assertEquals(BLOCK_ID, request.getCommand().getId());
      assertEquals(offset, request.getCommand().getOffset());
    } else {
      assertTrue(request.hasChunk());
    }
  }
}
