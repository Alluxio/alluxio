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

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nullable;

/**
 * A {@link DataReader} which serves data from a given byte array.
 */
public class TestDataReader implements DataReader {
  private final byte[] mData;
  private long mPos;
  private long mEnd;
  private long mChunkSize;
  private boolean mClosed = false;

  public TestDataReader(byte[] data, long chunkSize, long offset, long length) {
    mData = data;
    mChunkSize = chunkSize;
    mPos = offset;
    mEnd = offset + length;
  }

  @Override
  @Nullable
  public DataBuffer readChunk() {
    if (mPos >= mEnd || mPos >= mData.length) {
      return null;
    }
    int bytesToRead = (int) (Math.min(Math.min(mChunkSize, mEnd - mPos), mData.length - mPos));
    ByteBuffer buffer = ByteBuffer.wrap(mData, (int) mPos, bytesToRead);
    DataBuffer dataBuffer = new NioDataBuffer(buffer, buffer.remaining());
    mPos += dataBuffer.getLength();
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() {
    mClosed = true;
  }

  public boolean isClosed() {
    return mClosed;
  }

  public static class Factory implements DataReader.Factory {
    long mChunkSize;
    byte[] mData;
    TestDataReader mReader;

    public Factory(long chunkSize, byte[] data) {
      mChunkSize = chunkSize;
      mData = data;
    }

    @Override
    public DataReader create(long offset, long len) throws IOException {
      mReader = new TestDataReader(mData, mChunkSize, offset, len);
      return mReader;
    }

    @Override
    public void close() throws IOException {}

    public TestDataReader getDataReader() {
      return mReader;
    }
  }
}
