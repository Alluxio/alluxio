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
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A packet reader that simply reads packets from a local file.
 */
@NotThreadSafe
public final class LocalFilePacketReader implements PacketReader {
  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;

  private long mPos;
  private final long mEnd;
  private final long mPacketSize;

  /**
   * Creates an instance of {@link LocalFilePacketReader}.
   *
   * @param reader the local file block reader
   * @param offset the offset
   * @param len the length to read
   * @param packetSize the packet size
   */
  public LocalFilePacketReader(
      LocalFileBlockReader reader, long offset, long len, long packetSize) {
    Preconditions.checkArgument(packetSize > 0);
    mReader = reader;
    mPos = offset;
    mEnd = offset + len;
    mPacketSize = packetSize;
  }

  @Override
  public DataBuffer readPacket() {
    if (mPos >= mEnd) {
      return null;
    }
    ByteBuffer buffer = mReader.read(mPos, Math.min(mPacketSize, mEnd - mPos));
    DataBuffer dataBuffer = new DataByteBuffer(buffer, buffer.remaining());
    mPos += dataBuffer.getLength();
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() {
    mReader.close();
  }

  /**
   * Factory class to create {@link LocalFilePacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private final String mPath;
    private final long mPacketSize;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param path the file path
     * @param packetSize the packet size
     */
    public Factory(String path, long packetSize) {
      mPath = path;
      mPacketSize = packetSize;
    }

    @Override
    public PacketReader create(long offset, long len) {
      return new LocalFilePacketReader(new LocalFileBlockReader(mPath), offset, len, mPacketSize);
    }

    @Override
    public boolean isShortCircuit() {
      return true;
    }
  }
}

