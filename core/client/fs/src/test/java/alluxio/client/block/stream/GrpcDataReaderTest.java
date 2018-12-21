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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.client.file.FileSystemContext;
import alluxio.grpc.Chunk;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.netty.buffer.ByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, WorkerNetAddress.class})
public final class GrpcDataReaderTest {
  private static final int CHUNK_SIZE = 1024;
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

  private static final Random RANDOM = new Random();
  private static final long BLOCK_ID = 1L;

  private FileSystemContext mContext;
  private WorkerNetAddress mAddress;
  private BlockWorkerClient mClient;
  private GrpcDataReader.Factory mFactory;
  private Context mGrpcContext;

  @Before
  public void before() throws Exception {
    mContext = PowerMockito.mock(FileSystemContext.class);
    mAddress = mock(WorkerNetAddress.class);
    ReadRequest readRequest =
        ReadRequest.newBuilder().setBlockId(BLOCK_ID).setChunkSize(CHUNK_SIZE).build();
    mFactory = new GrpcDataReader.Factory(mContext, mAddress, readRequest);

    mClient = mock(BlockWorkerClient.class);
    PowerMockito.when(mContext.acquireBlockWorkerClient(mAddress)).thenReturn(mClient);
    PowerMockito.doNothing().when(mContext).releaseBlockWorkerClient(mAddress, mClient);
  }

  @After
  public void after() throws Exception {
    mClient.close();
  }

  /**
   * Reads an empty file.
   */
  @Test
  public void readEmptyFile() throws Exception {
    setReadResponses(mClient, 0, 0, 0);
    try (DataReader reader = create(0, 10)) {
      assertEquals(null, reader.readChunk());
    }
    validateReadRequestSent(mClient, 0, 10, false, CHUNK_SIZE);
  }

  /**
   * Reads all contents in a file.
   */
  @Test(timeout = 1000 * 60)
  public void readFullFile() throws Exception {
    long length = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    long checksum = setReadResponses(mClient, length, 0, length - 1);
    try (DataReader reader = create(0, length)) {
      long checksumActual = checkChunks(reader, 0, length);
      assertEquals(checksum, checksumActual);
    }
    validateReadRequestSent(mClient, 0, length, true, CHUNK_SIZE);
  }

  /**
   * Reads part of a file and checks the checksum of the part that is read.
   */
  @Test(timeout = 1000 * 60)
  public void readPartialFile() throws Exception {
    long length = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    long offset = 10;
    long checksumStart = 100;
    long bytesToRead = length / 3;
    long checksum = setReadResponses(mClient, length, checksumStart, bytesToRead - 1);

    try (DataReader reader = create(offset, length)) {
      long checksumActual = checkChunks(reader, checksumStart, bytesToRead);
      assertEquals(checksum, checksumActual);
    }
    validateReadRequestSent(mClient, offset, length, true, CHUNK_SIZE);
  }

  /**
   * Reads a file with unknown length.
   */
  @Test(timeout = 1000 * 60)
  public void fileLengthUnknown() throws Exception {
    long lengthActual = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    long checksumStart = 100;
    long bytesToRead = lengthActual / 3;
    long checksum = setReadResponses(mClient, lengthActual, checksumStart, bytesToRead - 1);
    try (DataReader reader = create(0, Long.MAX_VALUE)) {
      long checksumActual = checkChunks(reader, checksumStart, bytesToRead);
      assertEquals(checksum, checksumActual);
    }
    validateReadRequestSent(mClient, 0, Long.MAX_VALUE, true, CHUNK_SIZE);
  }

  /**
   * Creates a {@link DataReader}.
   *
   * @param offset the offset
   * @param length the length
   * @return the data reader instance
   */
  private DataReader create(long offset, long length) throws Exception {
    DataReader reader = mFactory.create(offset, length);
    return reader;
  }

  /**
   * Reads the chunks from the given {@link DataReader}.
   *
   * @param reader the data reader
   * @param checksumStart the start position to calculate the checksum
   * @param bytesToRead bytes to read
   * @return the checksum of the data read starting from checksumStart
   */
  private long checkChunks(DataReader reader, long checksumStart, long bytesToRead)
      throws Exception {
    long pos = 0;
    long checksum = 0;

    while (true) {
      DataBuffer chunk = reader.readChunk();
      if (chunk == null) {
        break;
      }
      try {
        assertTrue(chunk instanceof DataByteBuffer);
        ByteBuf buf = (ByteBuf) chunk.getNettyOutput();
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        for (int i = 0; i < bytes.length; i++) {
          if (pos >= checksumStart) {
            checksum += BufferUtils.byteToInt(bytes[i]);
          }
          pos++;
          if (pos >= bytesToRead) {
            return checksum;
          }
        }
      } finally {
        chunk.release();
      }
    }
    return checksum;
  }

  /**
   * Validates the read request sent.
   *
   * @param client the worker client
   * @param offset the offset
   * @param length the length
   */
  private void validateReadRequestSent(final BlockWorkerClient client, long offset, long length,
      boolean closed, int chunkSize) throws TimeoutException, InterruptedException {
    ArgumentCaptor<ReadRequest> captor = ArgumentCaptor.forClass(ReadRequest.class);
    verify(client).readBlock(captor.capture());
    ReadRequest readRequest = captor.getValue();
    assertTrue(readRequest != null);
    assertEquals(BLOCK_ID, readRequest.getBlockId());
    assertEquals(offset, readRequest.getOffset());
    assertEquals(length, readRequest.getLength());
    assertEquals(closed, mGrpcContext.isCancelled());
    assertEquals(chunkSize, readRequest.getChunkSize());
  }

  /**
   * Sets read responses which will be returned when readBlock() is invoked.
   *
   * @param client the worker client
   * @param length the length
   * @param start the start position to calculate the checksum
   * @param end the end position to calculate the checksum
   * @return the checksum
   */
  private long setReadResponses(final BlockWorkerClient client, final long length,
      final long start, final long end) {
    long checksum = 0;
    long pos = 0;

    long remaining = length;
    List<ReadResponse> responses = new ArrayList<ReadResponse>();
    while (remaining > 0) {
      int bytesToSend = (int) Math.min(remaining, CHUNK_SIZE);
      byte[] data = new byte[bytesToSend];
      RANDOM.nextBytes(data);
      responses.add(ReadResponse.newBuilder()
          .setChunk(Chunk.newBuilder().setData(ByteString.copyFrom(data))).build());
      remaining -= bytesToSend;

      for (int i = 0; i < data.length; i++) {
        if (pos >= start && pos <= end) {
          checksum += BufferUtils.byteToInt(data[i]);
        }
        pos++;
      }
    }
    PowerMockito.when(client.readBlock(any(ReadRequest.class)))
        .thenAnswer(args -> {
          mGrpcContext = Context.current();
          return responses.iterator();
        });

    return checksum;
  }
}
