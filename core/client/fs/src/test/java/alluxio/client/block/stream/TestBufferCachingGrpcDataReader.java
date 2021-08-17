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
  private int mTotalChunkNum = -1;

  TestBufferCachingGrpcDataReader(WorkerNetAddress address,
      CloseableResource<BlockWorkerClient> client, long dataTimeoutMs,
      ReadRequest readRequest, GrpcBlockingStream<ReadRequest, ReadResponse> stream,
      int chunkSize, int blockSize) {
    super(address, client, dataTimeoutMs, readRequest, stream);
    mChunkSize = chunkSize;
    mBlockSize = blockSize;
  }

  @Override
  protected DataBuffer readChunk() {
    if (mTotalChunkNum == -1) {
      mTotalChunkNum = getTotalChunkNum();
    }
    if (mReadChunkNum >= mTotalChunkNum) { // finished reading
      return null;
    }
    ByteBuffer byteBuffer = null;
    if ((mReadChunkNum + 1) * mChunkSize <= mBlockSize) { // full block read
      byteBuffer = BufferUtils
          .getIncreasingByteBuffer(mReadChunkNum * mChunkSize, mChunkSize);
    } else { // partial block read for the last chunk
      byteBuffer = BufferUtils.getIncreasingByteBuffer(
          mReadChunkNum * mChunkSize, mBlockSize - mReadChunkNum * mChunkSize);
    }
    DataBuffer buffer = new NioDataBuffer(byteBuffer, byteBuffer.remaining());
    mPosToRead += buffer.readableBytes();
    mReadChunkNum++;
    return buffer;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  public int getReadChunkNum() {
    return mReadChunkNum;
  }

  public boolean validateBuffer(int index, DataBuffer buffer) {
    if (mTotalChunkNum == -1) {
      mTotalChunkNum = getTotalChunkNum();
    }
    if (buffer == null) {
      return index >= mTotalChunkNum;
    }
    int bufferSize = buffer.readableBytes();
    if (index < mTotalChunkNum - 1 && bufferSize != mChunkSize) {
      return false;
    }
    if (index == mTotalChunkNum - 1
        && bufferSize != mBlockSize - (mTotalChunkNum - 1) * mChunkSize) {
      return false;
    }
    byte[] byteArray = new byte[bufferSize];
    buffer.readBytes(byteArray, 0, bufferSize);
    return BufferUtils.equalIncreasingByteArray(index * mChunkSize, bufferSize, byteArray);
  }

  public boolean validateBuffer(int start, int length, DataBuffer buffer) {
    if (buffer == null) {
      return start >= mBlockSize;
    }
    if (start + length >= mBlockSize) {
      return false;
    }
    if (buffer.readableBytes() != length) {
      return false;
    }
    byte[] byteArray = new byte[length];
    buffer.readBytes(byteArray, 0, length);
    return BufferUtils.equalIncreasingByteArray(start, length, byteArray);
  }

  public int getTotalChunkNum() {
    if (mBlockSize % mChunkSize == 0) {
      return mBlockSize / mChunkSize;
    }
    return mBlockSize / mChunkSize + 1;
  }
}
