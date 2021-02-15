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
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests a {@link BufferCachingGrpcDataReader}.
 */
public class BufferCachingGrpcDataReaderTest {
  private static final int BLOCK_SIZE = 1024 * 3 + 1024 / 3;
  private static final int CHUNK_SIZE = 1024;
  private static final long TIMEOUT = 10 * Constants.SECOND_MS;

  private TestBufferCachingGrpcDataReader mDataReader;

  @Before
  public void before() throws Exception {
    WorkerNetAddress address = new WorkerNetAddress();
    BlockWorkerClient client = Mockito.mock(BlockWorkerClient.class);
    GrpcBlockingStream<ReadRequest, ReadResponse> unusedStream
        = new GrpcBlockingStream<>(client::readBlock, 5, "test message");
    ReadRequest readRequest = ReadRequest.newBuilder().setOffset(0).setLength(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE).setBlockId(1L).build();
    mDataReader = new TestBufferCachingGrpcDataReader(address,
        new NoopClosableResource<>(client), TIMEOUT,
        readRequest, unusedStream, CHUNK_SIZE, BLOCK_SIZE);
  }

  @Test
  public void testSequentialRead() throws Exception {
    int chunkNum = BLOCK_SIZE / CHUNK_SIZE;
    for (int i = 0; i < chunkNum; i++) {
      DataBuffer buffer = mDataReader.readChunk(i);
      Assert.assertEquals(i + 1, mDataReader.getReadChunkNum());
      Assert.assertTrue(mDataReader.validateBuffer(i, buffer));
    }
  }

  @Test
  public void testDataCache() throws Exception {
    int index = 2;
    DataBuffer buffer = mDataReader.readChunk(index);
    Assert.assertEquals(index + 1, mDataReader.getReadChunkNum());
    Assert.assertTrue(mDataReader.validateBuffer(index, buffer));

    index = 1;
    buffer = mDataReader.readChunk(index);
    Assert.assertEquals(3, mDataReader.getReadChunkNum());
    Assert.assertTrue(mDataReader.validateBuffer(index, buffer));

    index = 0;
    buffer = mDataReader.readChunk(index);
    Assert.assertEquals(3, mDataReader.getReadChunkNum());
    Assert.assertTrue(mDataReader.validateBuffer(index, buffer));

    index = 3;
    buffer = mDataReader.readChunk(index);
    Assert.assertEquals(index + 1, mDataReader.getReadChunkNum());
    Assert.assertTrue(mDataReader.validateBuffer(index, buffer));
  }

  @Test
  public void testOutOfBound() throws Exception {
    int chunkNum = BLOCK_SIZE / CHUNK_SIZE + 1;
    for (int i = chunkNum; i < chunkNum + 10; i++) {
      DataBuffer buffer = mDataReader.readChunk(i);
      Assert.assertEquals(0, mDataReader.getReadChunkNum());
      Assert.assertNull(buffer);
    }
  }

  @Test
  public void testClassReference() {
    Assert.assertEquals(0, mDataReader.getRefCount());
    mDataReader.ref();
    Assert.assertEquals(1, mDataReader.getRefCount());
    mDataReader.ref();
    Assert.assertEquals(2, mDataReader.getRefCount());
    mDataReader.deRef();
    Assert.assertEquals(1, mDataReader.getRefCount());
    mDataReader.deRef();
    Assert.assertEquals(0, mDataReader.getRefCount());
    mDataReader.ref();
    Assert.assertEquals(1, mDataReader.getRefCount());
  }
}
