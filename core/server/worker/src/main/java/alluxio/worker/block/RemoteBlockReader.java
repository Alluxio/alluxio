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

import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.io.BlockReader;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Reads a block from a remote worker node.
 */
public class RemoteBlockReader implements BlockReader {
  private final long mBlockId;
  private final InetSocketAddress mDataSource;
  private final Protocol.OpenUfsBlockOptions mUfsOptions;

  private boolean mClosed;

  /**
   * Constructs a block cacher. Based on the data source, it will read from the under storage
   * system or a remote worker.
   * @param blockId the block to cache
   * @param dataSource the data source to cache from
   * @param ufsOptions the options to read the block from ufs if necessary
   */
  public RemoteBlockReader(long blockId, InetSocketAddress dataSource, Protocol.OpenUfsBlockOptions
      ufsOptions) {
    mBlockId = blockId;
    mDataSource = dataSource;
    mUfsOptions = ufsOptions;
    mClosed = false;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    return null;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public ReadableByteChannel getChannel() {
    return null;
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    return 0;
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public void close() throws IOException {

  }
}
