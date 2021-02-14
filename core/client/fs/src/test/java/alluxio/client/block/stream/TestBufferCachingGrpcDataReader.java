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

import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.resource.CloseableResource;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link BufferCachingGrpcDataReader} which serves data from
 * a manually created increasing byte buffer. It helps recording
 * the number of read operations and validates the read results are correct.
 */
public class TestBufferCachingGrpcDataReader extends BufferCachingGrpcDataReader {
  private int mReadChunkNum = 0;
  private int mChunkSize;
  private int mBlockSize;

  TestBufferCachingGrpcDataReader(WorkerNetAddress address,
      CloseableResource<BlockWorkerClient> client, long dataTimeoutMs,
      ReadRequest readRequest, GrpcBlockingStream<ReadRequest, ReadResponse> stream,
      int chunkSize, int blockSize) {
    super(address, client, dataTimeoutMs, readRequest, stream);
    mChunkSize = chunkSize;
    mBlockSize = blockSize;
  }

  @Override
  DataBuffer readChunk() {
    if (mReadChunkNum > mBlockSize / mChunkSize) {
      return null;
    }
    mReadChunkNum++;
    ByteBuffer byteBuffer = BufferUtils
        .getIncreasingByteBuffer(mReadChunkNum * mChunkSize, mChunkSize);
    DataBuffer buffer = new NioDataBuffer(byteBuffer, byteBuffer.remaining());
    mPosToRead += buffer.readableBytes();
    return buffer;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  public int getReadChunkNum() {
    return mReadChunkNum;
  }

  public boolean validateBufferResult(int index, DataBuffer buffer) {
    byte[] byteArray = new byte[mChunkSize];
    buffer.readBytes(byteArray, 0, mChunkSize);
    return BufferUtils.equalIncreasingByteArray(index * mChunkSize, mChunkSize, byteArray);
  }

  public boolean validateBufferResult(int start, int length, DataBuffer buffer) {
    if (buffer == null) {
      return false;
    }
    byte[] byteArray = new byte[length];
    buffer.readBytes(byteArray, 0, length);
    return BufferUtils.equalIncreasingByteArray(start, length, byteArray);
  }
}
