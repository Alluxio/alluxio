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
import alluxio.client.file.dora.netty.PartialReadException;
import alluxio.file.ReadTargetBuffer;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link PositionReader} that reads data through network from source.
 */
@ThreadSafe
public class DoraCachePositionReader implements PositionReader {
  private static final Logger LOG = LoggerFactory.getLogger(DoraCachePositionReader.class);
  private final PositionReader mNettyReader;
  private final long mFileLength;
  private final Optional<CloseableSupplier<PositionReader>> mFallbackReader;
  private volatile boolean mClosed;

  /**
   * @param dataReader     reader to read data through network
   * @param length         file length
   * @param fallbackReader the position reader to fallback to when errors happen,
   *    or none if no fallback read is needed
   */
  public DoraCachePositionReader(PositionReader dataReader,
      long length, Optional<CloseableSupplier<PositionReader>> fallbackReader) {
    mNettyReader = dataReader;
    mFileLength = length;
    mFallbackReader = fallbackReader;
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    if (position >= mFileLength) { // at end of file
      return -1;
    }
    int originalOffset = buffer.offset();
    try {
      return mNettyReader.read(position, buffer, length);
    } catch (PartialReadException e) {
      int bytesRead = e.getBytesRead();
      if (bytesRead == 0) {
        LOG.debug("Failed to read file from worker through Netty", e);
        buffer.offset(originalOffset);
        if (mFallbackReader.isPresent()) {
          return fallback(mFallbackReader.get().get(), position, buffer, length);
        } else {
          throw e;
        }
      }
      buffer.offset(originalOffset + bytesRead);
      return bytesRead;
    } catch (Throwable t) {
      LOG.debug("Failed to read file from worker through Netty", t);
      buffer.offset(originalOffset);
      if (mFallbackReader.isPresent()) {
        return fallback(mFallbackReader.get().get(), position, buffer, length);
      } else {
        throw t;
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mNettyReader.close();
    if (mFallbackReader.isPresent()) {
      mFallbackReader.get().close();
    }
  }

  private static int fallback(PositionReader fallbackReader, long position, ReadTargetBuffer buffer,
      int length) throws IOException {
    int read = fallbackReader.read(position, buffer, length);
    Metrics.UFS_FALLBACK_READ_BYTES.mark(read);
    Metrics.UFS_FALLBACK_COUNT.inc();
    return read;
  }

  private static class Metrics {
    static final Counter UFS_FALLBACK_COUNT =
        MetricsSystem.counter(MetricKey.CLIENT_UFS_FALLBACK_COUNT.getName());
    static final Meter UFS_FALLBACK_READ_BYTES =
        MetricsSystem.meter(MetricKey.CLIENT_UFS_FALLBACK_READ_BYTES.getName());
  }
}
