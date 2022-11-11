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
import alluxio.qos.RateLimiter;
import alluxio.worker.block.qos.BlockStoreRateLimiter;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * A {@link BlockReader} with RateLimit.
 */
public class RateLimitedBlockReader extends BlockReader {

  private final BlockReader mDelegate;
  private final RateLimiter mRateLimiter;

  /**
   * Constructs a new instance for {@link RateLimitedBlockReader}.
   * @param delegate real BlockReader
   * @param configuration conf
   */
  public RateLimitedBlockReader(BlockReader delegate, AlluxioConfiguration configuration) {
    mDelegate = delegate;
    mRateLimiter = BlockStoreRateLimiter.getReadLimiter(configuration);
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    mRateLimiter.acquire((int) length);
    return mDelegate.read(offset, length);
  }

  @Override
  public long getLength() {
    return mDelegate.getLength();
  }

  @Override
  public ReadableByteChannel getChannel() {
    return new RateLimitedReadableByteChannel(mDelegate.getChannel(), mRateLimiter);
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    mRateLimiter.acquire(buf.writableBytes());
    return mDelegate.transferTo(buf);
  }

  @Override
  public boolean isClosed() {
    return mDelegate.isClosed();
  }

  @Override
  public String getLocation() {
    return mDelegate.getLocation();
  }

  @Override
  public void close() throws IOException {
    mDelegate.close();
  }

  /**
   * Returns the delegated BlockReader.
   * @return block reader
   */
  public BlockReader getDelegate() {
    return mDelegate;
  }

  /**
   * A {@link ReadableByteChannel} with RateLimit.
   */
  public static class RateLimitedReadableByteChannel implements ReadableByteChannel {

    private final ReadableByteChannel mDelegate;

    private final RateLimiter mRateLimiter;

    /**
     * Constructs a new instance for {@link RateLimitedReadableByteChannel}.
     * @param delegate
     * @param rateLimiter
     */
    public RateLimitedReadableByteChannel(ReadableByteChannel delegate, RateLimiter rateLimiter) {
      mDelegate = delegate;
      mRateLimiter = rateLimiter;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      mRateLimiter.acquire(dst.remaining());
      return mDelegate.read(dst);
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
