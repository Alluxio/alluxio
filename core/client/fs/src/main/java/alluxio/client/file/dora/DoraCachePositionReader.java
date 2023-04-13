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

package alluxio.client.file.dora;

import alluxio.CloseableSupplier;
import alluxio.PositionReader;
import alluxio.client.file.dora.netty.NettyDataReader;
import alluxio.file.ReadTargetBuffer;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link PositionReader} that reads data through network from source.
 */
@ThreadSafe
public class DoraCachePositionReader implements PositionReader {
  private static final Logger LOG = LoggerFactory.getLogger(DoraCachePositionReader.class);
  private static final Counter UFS_FALLBACK_COUNTER = MetricsSystem.counter(
      MetricKey.CLIENT_UFS_FALLBACK_COUNT.getName());

  private final NettyDataReader mNettyReader;
  private final long mFileLength;
  private final CloseableSupplier<PositionReader> mFallbackReader;
  private volatile boolean mClosed;

  /**
   * @param dataReader reader to read data through network
   * @param length file length
   * @param fallbackReader the position reader to fallback to when errors happen
   */
  // TODO(lu) structure for fallback position read
  public DoraCachePositionReader(NettyDataReader dataReader,
      long length, CloseableSupplier<PositionReader> fallbackReader) {
    mNettyReader = dataReader;
    mFileLength = length;
    mFallbackReader = fallbackReader;
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    Preconditions.checkArgument(length >= 0, "length should be non-negative");
    Preconditions.checkArgument(position >= 0, "position should be non-negative");
    Preconditions.checkArgument(!mClosed, "position reader is closed");
    if (length == 0) {
      return 0;
    }
    if (position >= mFileLength) { // at end of file
      return -1;
    }
    try {
      return mNettyReader.read(position, buffer, length);
    } catch (Throwable t) {
      UFS_FALLBACK_COUNTER.inc();
      LOG.debug("Dora client read file error ({} times). Fall back to UFS.",
          UFS_FALLBACK_COUNTER.getCount(), t);
      return mFallbackReader.get().read(position, buffer, length);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mNettyReader.close();
    mFallbackReader.close();
  }
}
