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

import alluxio.Constants;
import alluxio.collections.ConcurrentHashSet;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Tests {@link SharedGrpcDataReaderTest}.
 */
public final class SharedGrpcDataReaderTest {
  private static final long BLOCK_ID = 1L;
  private static final int CHUNK_SIZE = 1024;
  private static final long TIMEOUT = 10 * Constants.SECOND_MS;

  private int mBlockSize = CHUNK_SIZE * 7 + CHUNK_SIZE / 3;
  private TestBufferCachingGrpcDataReader mBufferCachingDataReader;
  private ReadRequest mReadRequest;

  @Before
  public void before() {
    WorkerNetAddress address = new WorkerNetAddress();
    BlockWorkerClient client = Mockito.mock(BlockWorkerClient.class);
    GrpcBlockingStream<ReadRequest, ReadResponse> unusedStream
        = new GrpcBlockingStream<>(client::readBlock, 5, "test message");
    mReadRequest = ReadRequest.newBuilder().setOffset(0).setLength(mBlockSize)
        .setChunkSize(CHUNK_SIZE).setBlockId(BLOCK_ID).build();
    mBufferCachingDataReader = new TestBufferCachingGrpcDataReader(address,
        new NoopClosableResource<>(client), TIMEOUT,
        mReadRequest, unusedStream, CHUNK_SIZE, mBlockSize);
  }

  @Test
  public void singleThreadReadFullFile() throws Exception {
    SharedGrpcDataReader sharedReader
        = new SharedGrpcDataReader(mReadRequest, mBufferCachingDataReader);
    for (int i = 0; i < getChunkNum(mBlockSize); i++) {
      Assert.assertTrue(mBufferCachingDataReader.validateBuffer(i, sharedReader.readChunk()));
    }
    Assert.assertNull(sharedReader.readChunk());
  }

  @Test(timeout = 1000 * 60)
  public void singleThreadReadPartialFile() throws Exception {
    int[][] moves = {{10, 2438}, {1024, 2096}, {2042, 1222}, {3057, 2}};

    for (int[] offsetAndLen : moves) {
      int offset = offsetAndLen[0];
      int len = offsetAndLen[1];
      ReadRequest partialReadRequest = ReadRequest.newBuilder().setOffset(offset).setLength(len)
          .setBlockId(mReadRequest.getBlockId()).setChunkSize(mReadRequest.getChunkSize()).build();
      SharedGrpcDataReader sharedReader
          = new SharedGrpcDataReader(partialReadRequest, mBufferCachingDataReader);
      while (offset != -1) {
        offset = validateRead(sharedReader, offset, getChunkNum(len));
      }
    }
  }

  @Test(timeout = 1000 * 60)
  public void twoThreadSequentialRead() throws Exception {
    int readerOneOffset = 10;
    int readerOneLen = CHUNK_SIZE * 6 + CHUNK_SIZE / 3;
    int readerTwoOffset = 80;
    int readerTwoLen = CHUNK_SIZE * 5 + CHUNK_SIZE / 2;
    ReadRequest readRequestOne = ReadRequest.newBuilder()
        .setBlockId(BLOCK_ID).setChunkSize(CHUNK_SIZE)
        .setOffset(readerOneOffset).setLength(readerOneLen).build();
    ReadRequest readRequestTwo = ReadRequest.newBuilder()
        .setBlockId(BLOCK_ID).setChunkSize(CHUNK_SIZE)
        .setOffset(readerTwoOffset).setLength(readerTwoLen).build();
    SharedGrpcDataReader sharedReaderOne
        = new SharedGrpcDataReader(readRequestOne, mBufferCachingDataReader);
    SharedGrpcDataReader sharedReaderTwo
        = new SharedGrpcDataReader(readRequestTwo, mBufferCachingDataReader);
    boolean readerOne = true;
    while (readerOneOffset != -1 && readerTwoOffset != -1) {
      if (readerOne) {
        readerOneOffset = validateRead(sharedReaderOne, readerOneOffset, getChunkNum(readerOneLen));
      } else {
        readerTwoOffset = validateRead(sharedReaderTwo, readerTwoOffset, getChunkNum(readerTwoLen));
      }
      readerOne = !readerOne;
    }
  }

  /**
   * 10 threads read from the same file with the shared cache concurrently.
   */
  @Test(timeout = 1000 * 60)
  public void MultiThreadConcurrentRead() throws Exception {
    int concurrency = 10;
    List<Thread> threads = new ArrayList<>(concurrency);
    // If there are exceptions, we will store them here
    final ConcurrentHashSet<Throwable> errors = new ConcurrentHashSet<>();
    Thread.UncaughtExceptionHandler exceptionHandler = (th, ex) -> errors.add(ex);
    for (int i = 0; i < concurrency; i++) {
      Thread t = new Thread(() -> {
        Random random = new Random();
        int offset = random.nextInt(mBlockSize);
        int len = random.nextInt(mBlockSize - offset);
        ReadRequest partialReadRequest = ReadRequest.newBuilder()
            .setOffset(offset).setLength(len).setBlockId(mReadRequest.getBlockId())
            .setChunkSize(mReadRequest.getChunkSize()).build();
        try (SharedGrpcDataReader reader
                 = new SharedGrpcDataReader(partialReadRequest, mBufferCachingDataReader)) {
          while (offset != -1) {
            offset = validateRead(reader, offset, getChunkNum(len));
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
    for (Thread t : threads) {
      t.join();
    }
  }

  private int validateRead(SharedGrpcDataReader reader, int offset, int chunkNum)
      throws Exception {
    int index = offset / CHUNK_SIZE;
    Assert.assertTrue(mBufferCachingDataReader.validateBuffer(offset,
        (index + 1) * CHUNK_SIZE - offset, reader.readChunk()));
    return index >= chunkNum ? -1 : (index + 1) * CHUNK_SIZE;
  }

  private int getChunkNum(int len) {
    if (len % CHUNK_SIZE == 0) {
      return len / CHUNK_SIZE;
    }
    return len / CHUNK_SIZE + 1;
  }
}
