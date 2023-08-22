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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Readable channel implementation for {@link BlockReader}s.
 * <br>
 * The channel can be safely read from multiple threads. Its closed state is independent of
 * that of the block reader that created it. Its internal position is shared with the block reader,
 * that is, calling {@link #read(ByteBuffer)} will advance the internal position of the block
 * reader, and subsequent calls to {@link BlockReader#transferTo(ByteBuf)} will start from where
 * the channel was left off.
 */
@ThreadSafe
public class BlockReadableChannel implements ReadableByteChannel {
  private final BlockReader mReader;
  private volatile boolean mClosed = false;

  /**
   * Creates a new channel from a block reader.
   *
   * @param reader reader
   */
  public BlockReadableChannel(BlockReader reader) {
    mReader = Preconditions.checkNotNull(reader, "reader");
  }

  // reading from this channel will also affect the internal position used by the
  // reader's transferTo method
  // the interface does not clearly state whether the position should be
  // shared between the channel and transferTo, and existing impls disagree with each other
  // todo(bowen): make the interface clear about internal position, possibly by adding a new
  //   getChannel(long start) method that has a separate position.
  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (mClosed) {
      throw new ClosedChannelException();
    }
    int maxReadable = dst.remaining();
    int position = dst.position();
    synchronized (this) {
      ByteBuf buf = Unpooled.wrappedBuffer(dst);
      buf.writerIndex(0);
      int bytesRead = mReader.transferTo(buf);
      Preconditions.checkState(bytesRead <= maxReadable, "buffer overflow");
      if (bytesRead > 0) {
        dst.position(position + bytesRead);
      }
      return bytesRead;
    }
  }

  @Override
  public boolean isOpen() {
    return !mClosed;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
  }
}
