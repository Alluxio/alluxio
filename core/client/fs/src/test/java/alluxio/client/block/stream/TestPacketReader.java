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

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

/**
 * A {@link PacketReader} which serves data from a given byte array.
 */
public class TestPacketReader implements PacketReader {
  private final byte[] mData;
  private long mPos;
  private long mEnd;
  private long mPacketSize = 128;

  public TestPacketReader(byte[] data, long offset, long length) {
    mData = data;
    mPos = offset;
    mEnd = offset + length;
  }

  @Override
  @Nullable
  public DataBuffer readPacket() {
    if (mPos >= mEnd) {
      return null;
    }
    ByteBuffer buffer =
        ByteBuffer.wrap(mData, (int) mPos, (int) (Math.min(mPacketSize, mEnd - mPos)));
    DataBuffer dataBuffer = new DataByteBuffer(buffer, buffer.remaining());
    mPos += dataBuffer.getLength();
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() { }
}
