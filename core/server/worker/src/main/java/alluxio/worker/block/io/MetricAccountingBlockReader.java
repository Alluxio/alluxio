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

import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * An reader class with metrics.
 */
public class MetricAccountingBlockReader extends BlockReader {
  private final BlockReader mBlockReader;

  /**
   * A decorator of BlockReader.
   * @param mblockReader block reader
   */
  public MetricAccountingBlockReader(BlockReader mblockReader) {
    mBlockReader = mblockReader;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    ByteBuffer buffer = mBlockReader.read(offset, length);
    int bytesReadFromCache = buffer.limit() - buffer.position();
    MetricsSystem.counter(MetricKey.WORKER_BYTES_READ_CACHE.getName()).inc(bytesReadFromCache);
    return buffer;
  }

  @Override
  public long getLength() {
    return mBlockReader.getLength();
  }

  @Override
  public ReadableByteChannel getChannel() {
    return new ReadableByteChannel() {
      private final ReadableByteChannel mDelegate = mBlockReader.getChannel();
      @Override
      public int read(ByteBuffer dst) throws IOException {
        int bytesRead = mDelegate.read(dst);
        if (bytesRead != -1) {
          MetricsSystem.counter(MetricKey.WORKER_BYTES_READ_CACHE.getName()).inc(bytesRead);
        }
        return bytesRead;
      }

      @Override
      public boolean isOpen() {
        return mDelegate.isOpen();
      }

      @Override
      public void close() throws IOException {
        mDelegate.close();
      }
    };
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    int bytesReadFromCache = mBlockReader.transferTo(buf);
    if (bytesReadFromCache != -1) {
      MetricsSystem.counter(MetricKey.WORKER_BYTES_READ_CACHE.getName()).inc(bytesReadFromCache);
    }
    return bytesReadFromCache;
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
    mBlockReader.close();
  }
}
