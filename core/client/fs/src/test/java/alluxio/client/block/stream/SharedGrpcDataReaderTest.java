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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.ConcurrentHashSet;
import alluxio.grpc.Chunk;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, WorkerNetAddress.class})
public final class SharedGrpcDataReaderTest {
  private static final int CHUNK_SIZE = 1024;
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);
  private static final Random RANDOM = new Random();
  private static final long BLOCK_ID = 1L;

  private FileSystemContext mContext;
  private WorkerNetAddress mAddress;
  private BlockWorkerClient mClient;
  private ClientCallStreamObserver<ReadRequest> mRequestObserver;

  @Before
  public void before() throws Exception {
    mContext = PowerMockito.mock(FileSystemContext.class);
    when(mContext.getClientContext())
        .thenReturn(ClientContext.create(ConfigurationTestUtils.defaults()));
    when(mContext.getClusterConf()).thenReturn(ConfigurationTestUtils.defaults());
    mAddress = mock(WorkerNetAddress.class);
    mClient = mock(BlockWorkerClient.class);
    mRequestObserver = mock(ClientCallStreamObserver.class);
    when(mContext.acquireBlockWorkerClient(mAddress)).thenReturn(
        new NoopClosableResource<>(mClient));
    when(mClient.readBlock(any(StreamObserver.class))).thenReturn(mRequestObserver);
    when(mRequestObserver.isReady()).thenReturn(true);
  }

  @After
  public void after() throws Exception {
    mClient.close();
  }

  /**
   * Reads all contents in a file.
   */
  @Test(timeout = 1000 * 60)
  public void singleThreadReadFullFile() throws Exception {
    ReadRequest readRequest =
        ReadRequest.newBuilder().setBlockId(BLOCK_ID).setChunkSize(CHUNK_SIZE).build();
    long length = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    SharedGrpcDataReader.Factory factory
        = new SharedGrpcDataReader.Factory(mContext, mAddress, readRequest, length);
    try (DataReader reader = factory.create(0, length)) {
      long checksum = setReadResponses(length, 0, length);
      long checksumActual = checkChunks(reader, 0, length);
      assertEquals(checksum, checksumActual);
    }
    validateReadRequestSent(0, length, true, CHUNK_SIZE);
  }

  /**
   * Reads part of a file and checks the checksum of the part that is read.
   */
  @Test(timeout = 1000 * 60)
  public void singleThreadReadPartialFile() throws Exception {
    long length = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    long offset = 10;
    long checksumStart = 100;
    long checksumEnd = length / 3;
    ReadRequest readRequest =
        ReadRequest.newBuilder().setBlockId(BLOCK_ID).setChunkSize(CHUNK_SIZE).build();
    SharedGrpcDataReader.Factory factory
        = new SharedGrpcDataReader.Factory(mContext, mAddress, readRequest, length + offset);

    try (DataReader reader = factory.create(offset, length)) {
      long checksum = setReadResponses(length + offset, checksumStart, checksumEnd);
      long checksumActual = checkChunks(reader, checksumStart - offset, checksumEnd - offset);
      assertEquals(checksum, checksumActual);
    }
    validateReadRequestSent(0, length + offset, true, CHUNK_SIZE);
  }

  /**
   * Two threads read from the same file with the shared cache.
   * Thread two starts reading after thread one finishes reading.
   */
  @Test(timeout = 1000 * 60)
  public void twoThreadSequentialRead() throws Exception {
    long fullLength = CHUNK_SIZE * 1025;
    long readerOneOffset = 10;
    long readerOneLen = CHUNK_SIZE * 1024 + CHUNK_SIZE / 3;
    long readerTwoOffset = 80;
    long readerTwoLen = CHUNK_SIZE * 512;
    long checksumStart = 100;
    long checksumEnd = CHUNK_SIZE * 256;
    ReadRequest readRequest =
        ReadRequest.newBuilder().setBlockId(BLOCK_ID).setChunkSize(CHUNK_SIZE).build();
    SharedGrpcDataReader.Factory factory
        = new SharedGrpcDataReader.Factory(mContext, mAddress, readRequest, fullLength);

    // SharedGrpcDataReader will always read from 0 to the maximum index being read
    try (DataReader readerOne = factory.create(readerOneOffset, readerOneLen);
         DataReader readerTwo = factory.create(readerTwoOffset, readerTwoLen)) {
      long checksum = setReadResponses(fullLength, checksumStart, checksumEnd);
      long checksumActualOne = checkChunks(readerOne,
          checksumStart - readerOneOffset, checksumEnd - readerOneOffset);
      long checksumActualTwo = checkChunks(readerTwo,
          checksumStart - readerTwoOffset, checksumEnd - readerTwoOffset);
      assertEquals(checksum, checksumActualOne);
      assertEquals(checksum, checksumActualTwo);
    }
    validateReadRequestSent(0, fullLength, true, CHUNK_SIZE);
  }

  /**
   * Two threads read from the same file with the shared cache.
   * Thread one and two read concurrently.
   */
  @Test(timeout = 1000 * 60)
  public void twoThreadSerializedRead() throws Exception {
    long fullLength = CHUNK_SIZE * 1025;
    long[] offsets = {10, 80};
    long[] lens = {CHUNK_SIZE * 1024 + CHUNK_SIZE / 3, CHUNK_SIZE * 512};
    long checksumStart = 100;
    long checksumEnd = CHUNK_SIZE * 256;

    long[] pos = new long[2];
    long[] checksumActual = new long[2];
    long[] checksumStarts = {checksumStart - offsets[0], checksumStart - offsets[1]};
    long[] checksumEnds = {checksumEnd - offsets[0], checksumEnd - offsets[1]};

    ReadRequest readRequest =
        ReadRequest.newBuilder().setBlockId(BLOCK_ID).setChunkSize(CHUNK_SIZE).build();
    SharedGrpcDataReader.Factory factory
        = new SharedGrpcDataReader.Factory(mContext, mAddress, readRequest, fullLength);

    try (DataReader readerOne = factory.create(offsets[0], lens[0]);
         DataReader readerTwo = factory.create(offsets[1], lens[1])) {
      long checksum = setReadResponses(fullLength, checksumStart, checksumEnd);
      while (true) {
        if (readOnceFromReaders(new DataReader[]{readerOne, readerTwo},
            pos, checksumActual, checksumStarts, checksumEnds)) {
          break;
        }
      }
      Assert.assertEquals(checksumActual[0], checksumActual[1]);
      assertEquals(checksum, checksumActual[0]);
    }
    validateReadRequestSent(0, fullLength, true, CHUNK_SIZE);
  }

  /**
   * 10 threads read from the same file with the shared cache concurrently.
   */
  @Test(timeout = 1000 * 60)
  public void MultiThreadConcurrentRead() throws Exception {
    // 10 thread, randomly create DataReader and read
    // We should get no errors.
    ReadRequest readRequest =
        ReadRequest.newBuilder().setBlockId(BLOCK_ID).setChunkSize(CHUNK_SIZE).build();
    int length = CHUNK_SIZE * 1024;
    SharedGrpcDataReader.Factory factory
        = new SharedGrpcDataReader.Factory(mContext, mAddress, readRequest, length);
    ConcurrentHashSet<Throwable> errors = concurrentRead(10, factory, length);
    assertErrorsSizeEquals(errors, 0);
  }

  private ConcurrentHashSet<Throwable> concurrentRead(int concurrency,
      SharedGrpcDataReader.Factory factory, int totalSize) throws Exception {
    List<Thread> threads = new ArrayList<>(concurrency);
    // If there are exceptions, we will store them here
    final ConcurrentHashSet<Throwable> errors = new ConcurrentHashSet<>();
    Thread.UncaughtExceptionHandler exceptionHandler = (th, ex) -> errors.add(ex);
    for (int i = 0; i < concurrency; i++) {
      Thread t = new Thread(() -> {
        Random random = new Random();
        int offset = random.nextInt(totalSize);
        int len = random.nextInt(totalSize - offset);
        try (DataReader reader = factory.create(offset, len)) {
          long checksum = checkChunks(reader, 0, len);
          long actualSum = 0;
          for (int index = 0; index < len; index++) {
            actualSum += offset + index;
          }
          if (checksum != actualSum) {
            throw new IOException("Failed to read data");
          }
        } catch (Exception e) {
          Throwables.throwIfUnchecked(e);
        }
      });
      t.setUncaughtExceptionHandler(exceptionHandler);
      threads.add(t);
    }
    Collections.shuffle(threads);
    for (Thread t : threads) {
      t.start();
    }

    Thread responseThread = new Thread(() ->
        setReadResponses(CHUNK_SIZE * 1024, 0, 0));
    responseThread.start();
    responseThread.join();

    for (Thread t : threads) {
      t.join();
    }

    return errors;
  }

  private void assertErrorsSizeEquals(ConcurrentHashSet<Throwable> errors, int expected) {
    if (errors.size() != expected) {
      Assert.fail(String.format("Expected %d errors, but got %d, errors:\n",
          expected, errors.size()) + Joiner.on("\n").join(errors));
    }
  }

  /**
   * Reads the chunks from the given {@link DataReader}.
   *
   * @param reader the data reader
   * @param checksumStart the start position to calculate the checksum
   * @param checksumEnd bytes to read
   * @return the checksum of the data read starting from checksumStart
   */
  private long checkChunks(DataReader reader, long checksumStart, long checksumEnd)
      throws Exception {
    long[] pos = new long[1];
    long[] checksum = new long[1];
    while (true) {
      if (readOnceFromReaders(new DataReader[]{reader}, pos, checksum,
          new long[]{checksumStart}, new long[]{checksumEnd})) {
        break;
      }
    }
    return checksum[0];
  }

  /**
   * Reads one chunk from each given reader.
   * If any data in the chunk read belongs to the checksum range, add to the checksum.
   *
   * @param reader readers to read chunks from
   * @param pos positions to check if the data belongs to the checksum range
   * @param checksum the calculated checksums
   * @param checksumStart the start index of the checksum range
   * @param checksumEnd the end index of the checksum range
   * @return true if reading should be finished, false otherwise
   */
  private boolean readOnceFromReaders(DataReader[] reader, long[] pos, long[] checksum,
      long[] checksumStart, long[] checksumEnd) throws IOException {
    boolean[] finished = new boolean[pos.length];
    for (int index = 0; index < pos.length; index++) {
      if (finished[index]) {
        continue;
      }
      DataBuffer chunk = reader[index].readChunk();
      if (chunk == null) {
        finished[index] = true;
        continue;
      }
      try {
        assertTrue(chunk instanceof NioDataBuffer);
        ByteBuf buf = (ByteBuf) chunk.getNettyOutput();
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        for (int i = 0; i < bytes.length; i++) {
          if (pos[index] >= checksumStart[index]) {
            checksum[index] += BufferUtils.byteToInt(bytes[i]);
          }
          pos[index]++;
          if (pos[index] > checksumEnd[index]) {
            finished[index] = true;
            break;
          }
        }
      } finally {
        chunk.release();
      }
    }

    boolean allFinished = true;
    for (boolean finish : finished) {
      if (!finish) {
        allFinished = false;
        break;
      }
    }
    return allFinished;
  }

  /**
   * Validates the read request sent.
   *
   * @param offset the offset
   * @param length the length
   */
  private void validateReadRequestSent(long offset, long length,
      boolean closed, int chunkSize) {
    ArgumentCaptor<ReadRequest> requestCaptor = ArgumentCaptor.forClass(ReadRequest.class);
    verify(mRequestObserver, atLeastOnce()).onNext(requestCaptor.capture());
    ArgumentCaptor<StreamObserver> captor = ArgumentCaptor.forClass(StreamObserver.class);
    verify(mClient).readBlock(captor.capture());
    List<ReadRequest> readRequests = requestCaptor.getAllValues();
    assertFalse(readRequests.isEmpty());
    captor.getValue().onCompleted();
    long lastOffset = offset;
    for (int i = 0; i < readRequests.size(); i++) {
      ReadRequest readRequest = readRequests.get(i);
      if (i == 0) {
        assertNotNull(readRequest);
        assertEquals(BLOCK_ID, readRequest.getBlockId());
        assertEquals(offset, readRequest.getOffset());
        assertEquals(length, readRequest.getLength());
        assertEquals(chunkSize, readRequest.getChunkSize());
      } else {
        assertTrue(readRequest.hasOffsetReceived());
        assertTrue(readRequest.getOffsetReceived() > lastOffset);
        assertTrue(readRequest.getOffsetReceived() <= length);
        lastOffset = readRequest.getOffsetReceived();
      }
    }
    verify(mRequestObserver, closed ? atLeastOnce() : never()).onCompleted();
  }

  /**
   * Sets read responses which will be returned when readBlock() is invoked.
   *
   * @param length the length
   * @param start the start position to calculate the checksum
   * @param end the end position to calculate the checksum
   * @return the checksum
   */
  private long setReadResponses(long length, long start, long end) {
    long checksum = 0;
    long pos = 0;

    long remaining = length;
    ArgumentCaptor<StreamObserver> captor = ArgumentCaptor.forClass(StreamObserver.class);
    verify(mClient).readBlock(captor.capture());
    StreamObserver<ReadResponse> responseObserver = captor.getValue();
    List<ReadResponse> responses = new ArrayList<>();
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
        if (pos > end) {
          break;
        }
      }
    }
    EXECUTOR.submit(() -> {
      for (ReadResponse response : responses) {
        responseObserver.onNext(response);
      }
      responseObserver.onCompleted();
    });
    return checksum;
  }
}
