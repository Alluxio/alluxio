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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link PositionReader} that reads data through network from source.
 */
@ThreadSafe
public class DoraCachePositionReader implements PositionReader {
  private static final Logger LOG = LoggerFactory.getLogger(DoraCachePositionReader.class);
  private final PositionReader mNettyReader;
  private final long mFileLength;
  private final CloseableSupplier<PositionReader> mFallbackReader;
  private volatile boolean mClosed;

  /**
   * @param dataReader reader to read data through network
   * @param length file length
   * @param fallbackReader the position reader to fallback to when errors happen
   */
  public DoraCachePositionReader(PositionReader dataReader,
      long length, @Nullable CloseableSupplier<PositionReader> fallbackReader) {
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
        if (mFallbackReader == null) {
          throw e;
        }
        return fallback(position, buffer, length);
      }
      buffer.offset(originalOffset + bytesRead);
      return bytesRead;
    } catch (Throwable t) {
      LOG.debug("Failed to read file from worker through Netty", t);
      buffer.offset(originalOffset);
      if (mFallbackReader == null) {
        throw t;
      }
      return fallback(position, buffer, length);
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

  private int fallback(long position, ReadTargetBuffer buffer,
      int length) throws IOException {
    int read = mFallbackReader.get().read(position, buffer, length);
    MetricsSystem.meter(MetricKey.CLIENT_WORKER_READ_UFS_FALLBACK_BYTES.getName())
        .mark(read);
    MetricsSystem.counter(MetricKey.CLIENT_WORKER_READ_UFS_FALLBACK.getName()).inc();
    return read;
  }
}
