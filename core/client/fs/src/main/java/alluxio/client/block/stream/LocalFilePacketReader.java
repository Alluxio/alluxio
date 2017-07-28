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
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.netty.NettyRPC;
import alluxio.client.netty.NettyRPCContext;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;

import java.io.IOException;
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

  private boolean mClosed;

  /**
   * Creates an instance of {@link LocalFilePacketReader}.
   *
   * @param path the block path
   * @param offset the offset
   * @param len the length to read
   * @param packetSize the packet size
   */
  private LocalFilePacketReader(String path, long offset, long len, long packetSize)
      throws IOException {
    mReader = new LocalFileBlockReader(path);
    Preconditions.checkArgument(packetSize > 0);
    mPos = offset;
    mEnd = offset + len;
    mPacketSize = packetSize;
  }

  @Override
  public DataBuffer readPacket() throws IOException {
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
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mReader.close();
  }

  /**
   * Factory class to create {@link LocalFilePacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private static final long READ_TIMEOUT_MS =
        Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

    private final FileSystemContext mContext;
    private final WorkerNetAddress mAddress;
    private final Channel mChannel;
    private final long mBlockId;
    private final String mPath;
    private final long mPacketSize;
    private boolean mClosed;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param context the file system context
     * @param address the worker address
     * @param blockId the block ID
     * @param packetSize the packet size
     * @param options the instream options
     */
    public Factory(FileSystemContext context, WorkerNetAddress address, long blockId,
        long packetSize, InStreamOptions options) throws IOException {
      mContext = context;
      mAddress = address;
      mBlockId = blockId;
      mPacketSize = packetSize;

      mChannel = context.acquireNettyChannel(address);
      Protocol.LocalBlockOpenRequest request =
          Protocol.LocalBlockOpenRequest.newBuilder().setBlockId(mBlockId)
              .setPromote(options.getAlluxioStorageType().isPromote()).build();
      try {
        ProtoMessage message = NettyRPC
            .call(NettyRPCContext.defaults().setChannel(mChannel).setTimeout(READ_TIMEOUT_MS),
                new ProtoMessage(request));
        Preconditions.checkState(message.isLocalBlockOpenResponse());
        mPath = message.asLocalBlockOpenResponse().getPath();
      } catch (Exception e) {
        context.releaseNettyChannel(address, mChannel);
        throw e;
      }
    }

    @Override
    public PacketReader create(long offset, long len) throws IOException {
      return new LocalFilePacketReader(mPath, offset, len, mPacketSize);
    }

    @Override
    public boolean isShortCircuit() {
      return true;
    }

    @Override
    public void close() throws IOException {
      if (mClosed) {
        return;
      }
      Protocol.LocalBlockCloseRequest request =
          Protocol.LocalBlockCloseRequest.newBuilder().setBlockId(mBlockId).build();
      try {
        NettyRPC.call(NettyRPCContext.defaults().setChannel(mChannel).setTimeout(READ_TIMEOUT_MS),
            new ProtoMessage(request));
      } finally {
        mClosed = true;
        mContext.releaseNettyChannel(mAddress, mChannel);
      }
    }
  }
}

