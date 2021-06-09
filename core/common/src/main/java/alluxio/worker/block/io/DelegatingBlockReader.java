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

package alluxio.worker.block.io;

import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * An delegating reader class.
 */
public class DelegatingBlockReader extends BlockReader {
  private final BlockReader mBlockReader;
  private final Closer mCloser;

  /**
   * Default constructor for the abstract reader implementations.
   * @param blockReader block reader
   * @param closeable closer
   */
  public DelegatingBlockReader(BlockReader blockReader, Closeable closeable) {
    mCloser = Closer.create();
    mBlockReader = mCloser.register(blockReader);
    mCloser.register(closeable);
  }

  /**
   * @return the delegate
   */
  public BlockReader getDelegate() {
    return mBlockReader;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    return mBlockReader.read(offset, length);
  }

  @Override
  public long getLength() {
    return mBlockReader.getLength();
  }

  @Override
  public ReadableByteChannel getChannel() {
    return mBlockReader.getChannel();
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    return mBlockReader.transferTo(buf);
  }

  @Override
  public boolean isClosed() {
    return mBlockReader.isClosed();
  }

  @Override
  public String getLocation() {
    return mBlockReader.getLocation();
  }

  @Override
  public String toString() {
    return mBlockReader.toString();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
