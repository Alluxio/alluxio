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
import alluxio.client.file.FileSystemContext;
import alluxio.client.netty.NettyRPC;
import alluxio.client.netty.NettyRPCContext;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A packet reader that simply reads packets from a local file.
 */
@NotThreadSafe
public final class LocalFilePacketReader implements PacketReader {
  private static final long LOCAL_READ_PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES);

  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;

  private long mPos;
  private final long mEnd;

  private boolean mClosed;

  /**
   * Creates an instance of {@link LocalFilePacketReader}.
   *
   * @param path the block path
   * @param offset the offset
   * @param len the length to read
   */
  private LocalFilePacketReader(String path, long offset, long len) {
    mReader = new LocalFileBlockReader(path);
    mPos = offset;
    mEnd = offset + len;
  }

  @Override
  public DataBuffer readPacket() {
    if (mPos >= mEnd) {
      return null;
    }

    ByteBuffer buffer = mReader.read(mPos, Math.min(LOCAL_READ_PACKET_SIZE, mEnd - mPos));
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
    if (mClosed) {
      return;
    }
    mClosed = true;
    mReader.close();;
  }

  /**
   * Factory class to create {@link LocalFilePacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private static final long READ_TIMEOUT_MS =
        Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

    private final Channel mChannel;
    private final long mBlockId;
    private final long mSessionId;
    private final String mPath;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param context the file system context
     * @param address the worker address
     * @param blockId the block ID
     * @param sessionId the session ID
     */
    public Factory(FileSystemContext context, InetSocketAddress address, long blockId,
        long sessionId) {
      mBlockId = blockId;
      mSessionId = sessionId;

      mChannel = context.acquireNettyChannel(address);
      ProtoMessage message = NettyRPC
          .call(NettyRPCContext.defaults().setChannel(mChannel).setTimeout(READ_TIMEOUT_MS),
              new ProtoMessage(Protocol.LocalBlockOpenRequest.newBuilder().setBlockId(mBlockId)
                  .setSessionId(mSessionId).build()));
      Preconditions.checkState(message.isLocalBlockOpenResponse());
      mPath = message.asLocalBlockOpenResponse().getPath();
    }

    @Override
    public PacketReader create(long offset, long len) {
      return new LocalFilePacketReader(mPath, offset, len);
    }

    @Override
    public boolean isShortCircuit() {
      return true;
    }

    @Override
    public void close() throws IOException {
      NettyRPC.call(
          NettyRPCContext.defaults().setChannel(mChannel).setTimeout(READ_TIMEOUT_MS),
          new ProtoMessage(
              Protocol.LocalBlockCloseRequest.newBuilder().setBlockId(mBlockId).build()));
    }
  }
}

