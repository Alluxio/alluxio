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

package alluxio.worker.block;

import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Reads a block from a remote worker node. This should only be used for reading entire blocks.
 */
public class RemoteBlockReader implements BlockReader {
  private final long mBlockId;
  private final long mBlockSize;
  private final InetSocketAddress mDataSource;
  private final Protocol.OpenUfsBlockOptions mUfsOptions;

  private BlockInStream mInputStream;
  private ReadableByteChannel mChannel;
  private boolean mClosed;

  /**
   * Constructs a remote block reader. It will read from a remote worker based on the data source.
   *
   * @param blockId the block to cache
   * @param blockSize the size of the block
   * @param dataSource the data source to cache from
   * @param ufsOptions the options to read the block from ufs if necessary
   */
  public RemoteBlockReader(long blockId, long blockSize, InetSocketAddress dataSource,
      Protocol.OpenUfsBlockOptions ufsOptions) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mDataSource = dataSource;
    mUfsOptions = ufsOptions;
    mClosed = false;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    throw new UnsupportedOperationException("RemoteBlockReader#read is not supported");
  }

  @Override
  public long getLength() {
    return mUfsOptions.getBlockSize();
  }

  @Override
  public ReadableByteChannel getChannel() {
    Preconditions.checkState(!mClosed);
    init();
    return mChannel;
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    Preconditions.checkState(!mClosed);
    init();
    if (mInputStream == null || mInputStream.remaining() <= 0) {
      return -1;
    }
    int bytesToRead = (int) Math.min(buf.writableBytes(), mInputStream.remaining());
    return buf.writeBytes(mInputStream, bytesToRead);
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mInputStream != null) {
      mInputStream.close();
      mChannel.close();
    }
    mClosed = true;
  }

  private void init() {
    if (mInputStream != null) {
      return;
    }
    WorkerNetAddress address = new WorkerNetAddress().setHost(mDataSource.getHostName())
        .setDataPort(mDataSource.getPort());
    mInputStream = BlockInStream.createRemoteBlockInStream(FileSystemContext.get(), mBlockId,
        address, BlockInStream.BlockInStreamSource.REMOTE, mBlockSize, mUfsOptions);
    mChannel = Channels.newChannel(mInputStream);
  }
}
