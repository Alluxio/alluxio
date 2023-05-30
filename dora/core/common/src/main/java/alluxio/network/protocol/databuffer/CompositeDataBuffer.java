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

package alluxio.network.protocol.databuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A CompositeDataBuffer which includes a {@link List} of {@link DataBuffer}.
 */
public final class CompositeDataBuffer implements DataBuffer {

  private final List<DataBuffer> mDataBufferList;

  /**
   * CompositeDataBuffer wraps multiple {@link DataBuffer}.
   * @param dataBufferList a list of {@link DataBuffer}
   */
  public CompositeDataBuffer(List<DataBuffer> dataBufferList) {
    mDataBufferList = dataBufferList;
  }

  @Override
  public Object getNettyOutput() {
    return mDataBufferList;
  }

  @Override
  public long getLength() {
    long totalLength = 0L;
    for (DataBuffer dataBuffer : mDataBufferList) {
      totalLength += dataBuffer.getLength();
    }
    return totalLength;
  }

  @Override
  public ByteBuffer getReadOnlyByteBuffer() {
    throw new UnsupportedOperationException(
        "DataFileChannel#getReadOnlyByteBuffer is not implemented.");
  }

  @Override
  public void readBytes(byte[] dst, int dstIndex, int length) {
    throw new UnsupportedOperationException("DataFileChannel#readBytes is not implemented.");
  }

  @Override
  public void readBytes(OutputStream outputStream, int length) throws IOException {
    throw new UnsupportedOperationException("DataFileChannel#readBytes is not implemented.");
  }

  @Override
  public void readBytes(ByteBuffer outputBuf) {
    throw new UnsupportedOperationException("DataFileChannel#readBytes is not implemented.");
  }

  @Override
  public int readableBytes() {
    long totalReadableByres = 0L;
    for (DataBuffer dataBuffer : mDataBufferList) {
      totalReadableByres += dataBuffer.readableBytes();
    }
    return (int) totalReadableByres;
  }

  @Override
  public void release() {
    for (DataBuffer dataBuffer : mDataBufferList) {
      dataBuffer.release();
    }
  }
}
