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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.grpc.Chunk;
import alluxio.grpc.RequestType;
import alluxio.grpc.WriteRequest;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
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

  private static final int PACKET_SIZE = 1024;
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4,
      ThreadFactoryUtils.build("test-executor-%d", true));

  private static final Random RANDOM = new Random();
  private static final long BLOCK_ID = 1L;
  private static final int TIER = 0;

  private FileSystemContext mContext;
  private WorkerNetAddress mAddress;
  private BlockWorkerClient mClient;
  private ClientCallStreamObserver<WriteRequest> mRequestObserver;
  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES,
          String.valueOf(PACKET_SIZE));

  @Before
  public void before() throws Exception {
    mContext = PowerMockito.mock(FileSystemContext.class);
    mAddress = mock(WorkerNetAddress.class);

    mClient = mock(BlockWorkerClient.class);
    mRequestObserver = mock(ClientCallStreamObserver.class);
    PowerMockito.when(mContext.acquireBlockWorkerClient(mAddress)).thenReturn(mClient);
    PowerMockito.doNothing().when(mContext).releaseBlockWorkerClient(mAddress, mClient);
    PowerMockito.when(mClient.writeBlock(any(StreamObserver.class))).thenReturn(mRequestObserver);
    PowerMockito.when(mRequestObserver.isReady()).thenReturn(true);
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
    try (PacketWriter writer = create(10)) {
      checksumActual = verifyWriteRequests(mClient, 0, 10);
    }
    Assert.assertEquals(0, checksumActual);
  }

  /**
   * Writes a file with file length matches what is given and verifies the checksum of the whole
   * file.
   */
  @Test(timeout = 1000 * 60)
  public void writeFullFile() throws Exception {
    long checksumActual;
    Future<Long> checksumExpected;
    long length = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(length)) {
      checksumExpected = writeFile(writer, length, 0, length - 1);
      checksumExpected.get();
      checksumActual = verifyWriteRequests(mClient, 0, length - 1);
    }
    Assert.assertEquals(checksumExpected.get().longValue(), checksumActual);
  }

  /**
   * Writes a file with file length matches what is given and verifies the checksum of the whole
   * file.
   */
  @Test(timeout = 1000 * 60)
  public void writeFileChecksumOfPartialFile() throws Exception {
    long checksumActual;
    Future<Long> checksumExpected;
    long length = PACKET_SIZE * 1024 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(length)) {
      checksumExpected = writeFile(writer, length, 10, length / 3);
      checksumExpected.get();
      checksumActual = verifyWriteRequests(mClient, 10, length / 3);
    }
    Assert.assertEquals(checksumExpected.get().longValue(), checksumActual);
  }

  /**
   * Writes a file with unknown length.
   */
  @Test(timeout = 1000 * 60)
  public void writeFileUnknownLength() throws Exception {
    long checksumActual;
    Future<Long> checksumExpected;
    long length = PACKET_SIZE * 1024;
    try (PacketWriter writer = create(Long.MAX_VALUE)) {
      checksumExpected = writeFile(writer, length, 10, length / 3);
      checksumExpected.get();
      checksumActual = verifyWriteRequests(mClient, 10, length / 3);
    }
    Assert.assertEquals(checksumExpected.get().longValue(), checksumActual);
  }

  /**
   * Writes lots of packets.
   */
  @Test(timeout = 1000 * 60)
  public void writeFileManyPackets() throws Exception {
    long checksumActual;
    Future<Long> checksumExpected;
    long length = PACKET_SIZE * 30000 + PACKET_SIZE / 3;
    try (PacketWriter writer = create(Long.MAX_VALUE)) {
      checksumExpected = writeFile(writer, length, 10, length / 3);
      checksumExpected.get();
      checksumActual = verifyWriteRequests(mClient, 10, length / 3);
    }
    Assert.assertEquals(checksumExpected.get().longValue(), checksumActual);
  }

  /**
   * Creates a {@link PacketWriter}.
   *
   * @param length the length
   * @return the packet writer instance
   */
  private PacketWriter create(long length) throws Exception {
    PacketWriter writer =
        GrpcDataWriter.create(mContext, mAddress, BLOCK_ID, length,
            RequestType.ALLUXIO_BLOCK,
            OutStreamOptions.defaults().setWriteTier(TIER));
    return writer;
  }

  /**
   * Writes packets via the given packet writer and returns a checksum for a region of the data
   * written.
   *
   * @param length the length
   * @param start the start position to calculate the checksum
   * @param end the end position to calculate the checksum
   * @return the checksum
   */
  private Future<Long> writeFile(final PacketWriter writer, final long length,
      final long start, final long end) throws Exception {
    return EXECUTOR.submit(new Callable<Long>() {
      @Override
      public Long call() throws IOException {
        try {
          long checksum = 0;
          long pos = 0;

          long remaining = length;
          while (remaining > 0) {
            int bytesToWrite = (int) Math.min(remaining, PACKET_SIZE);
            byte[] data = new byte[bytesToWrite];
            RANDOM.nextBytes(data);
            ByteBuf buf = Unpooled.wrappedBuffer(data);
            try {
              writer.writePacket(buf);
            } catch (Exception e) {
              Assert.fail(e.getMessage());
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
          Assert.fail();
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
          Assert.assertTrue(chunk.hasData());
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
      Assert.fail();
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
      Assert.assertEquals(BLOCK_ID, request.getCommand().getId());
      Assert.assertEquals(offset, request.getCommand().getOffset());
    } else {
      Assert.assertTrue(request.hasChunk());
    }
  }
}
