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
import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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

  private final NettyDataReader mReader;
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
    mReader = dataReader;
    mFileLength = length;
    mFallbackReader = fallbackReader;
  }

  @Override
  public int positionReadInternal(long position, PageReadTargetBuffer buffer, int length)
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
      mReader.readFully(position, buffer.byteChannel(), length);
    } catch (PartialReadException e) {
      if (e.getCauseType() == PartialReadException.CauseType.EARLY_EOF) {
        if (e.getBytesRead() > 0) {
          return e.getBytesRead();
        } else {
          return -1;
        }
      }
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new IOException(e.getCause());
    } catch (RuntimeException e) {
      UFS_FALLBACK_COUNTER.inc();
      LOG.debug("Dora client read file error ({} times). Fall back to UFS.",
          UFS_FALLBACK_COUNTER.getCount(), e);
      // TODO(lu) what if read partial failed, cleanup the buffer?
      return mFallbackReader.get().positionRead(position, buffer, length);
    }
    return length;
  }

  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mReader.close();
    mFallbackReader.close();
  }
}
