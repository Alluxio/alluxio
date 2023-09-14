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

import alluxio.conf.AlluxioConfiguration;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.qos.RateLimiter;
import alluxio.worker.block.qos.BlockStoreRateLimiter;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A {@link BlockWriter} with RateLimit.
 */
public class RateLimitedBlockWriter extends BlockWriter {

  private final BlockWriter mDelegate;
  private final RateLimiter mRateLimiter;

  /**
   * Constructs a new instance for {@link RateLimitedBlockWriter}.
   * @param blockWriter the real BlockWriter
   * @param configuration conf
   */
  public RateLimitedBlockWriter(BlockWriter blockWriter, AlluxioConfiguration configuration) {
    mDelegate = blockWriter;
    mRateLimiter = BlockStoreRateLimiter.getWriteLimiter(configuration);
  }

  @Override
  public long append(ByteBuffer inputBuf) {
    mRateLimiter.acquire(inputBuf.remaining());
    return mDelegate.append(inputBuf);
  }

  @Override
  public long append(ByteBuf buf) throws IOException {
    mRateLimiter.acquire(buf.readableBytes());
    return mDelegate.append(buf);
  }

  @Override
  public long append(DataBuffer buffer) throws IOException {
    mRateLimiter.acquire(buffer.readableBytes());
    return mDelegate.append(buffer);
  }

  @Override
  public long getPosition() {
    return mDelegate.getPosition();
  }

  @Override
  public WritableByteChannel getChannel() {
    return new RateLimitedWritableByteChannel(mDelegate.getChannel(), mRateLimiter);
  }

  @Override
  public void close() throws IOException {
    mDelegate.close();
  }

  /**
   * A {@link WritableByteChannel} with RateLimit.
   */
  public static class RateLimitedWritableByteChannel implements WritableByteChannel {

    private final WritableByteChannel mDelegate;
    private final RateLimiter mRateLimiter;

    /**
     * Constructs a new instance for {@link RateLimitedWritableByteChannel}.
     * @param delegate
     * @param rateLimiter
     */
    public RateLimitedWritableByteChannel(WritableByteChannel delegate, RateLimiter rateLimiter) {
      mDelegate = delegate;
      mRateLimiter = rateLimiter;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      mRateLimiter.acquire(src.remaining());
      return mDelegate.write(src);
    }

    @Override
    public boolean isOpen() {
      return mDelegate.isOpen();
    }

    @Override
    public void close() throws IOException {
      mDelegate.close();
    }
  }
}
