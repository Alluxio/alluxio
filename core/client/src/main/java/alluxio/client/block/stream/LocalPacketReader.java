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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.worker.block.io.LocalFileBlockReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local packet reader that simply reads packets from a local file.
 */
@NotThreadSafe
public final class LocalPacketReader implements PacketReader {
  private static final long LOCAL_READ_PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES);

  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;

  private long mPos;
  private final long mEnd;

  /**
   * Creates an instance of {@link LocalPacketReader}.
   *
   * @param reader the local file block reader
   * @param offset the offset
   * @param len the length to read
   */
  public LocalPacketReader(LocalFileBlockReader reader, long offset, long len) {
    mReader = reader;
    mPos = offset;
    mEnd = offset + len;
  }

  @Override
  public ByteBuf readPacket() throws IOException {
    if (mPos >= mEnd) {
      return null;
    }
    ByteBuf buf =
        Unpooled.wrappedBuffer(mReader.read(mPos, Math.min(LOCAL_READ_PACKET_SIZE, mEnd - mPos)));
    mPos += buf.readableBytes();
    return buf;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() {}

  /**
   * Factory class to create {@link LocalPacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private final LocalFileBlockReader mLocalFileBlockReader;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param reader the local file block reader
     */
    public Factory(LocalFileBlockReader reader) {
      mLocalFileBlockReader = reader;
    }

    @Override
    public PacketReader create(long offset, long len) {
      return new LocalPacketReader(mLocalFileBlockReader, offset, len);
    }
  }
}

