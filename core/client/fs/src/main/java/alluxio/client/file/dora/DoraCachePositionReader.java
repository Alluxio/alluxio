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

import alluxio.PositionReader;
import alluxio.client.block.stream.NettyDataReader;
import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
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

  private final NettyDataReader.Factory mReaderFactory;
  private final long mFileLength;
  private final Supplier<PositionReader>  mExternalReader;

  /**
   * @param readerFactory reader to read data through network
   * @param length file length
   * @param externalReader the external reader
   */
  // TODO(lu) structure for fallback position read
  public DoraCachePositionReader(NettyDataReader.Factory readerFactory,
      long length, Supplier<PositionReader> externalReader) {
    mReaderFactory = readerFactory;
    mFileLength = length;
    mExternalReader = externalReader;
  }

  @Override
  public int positionRead(long position, PageReadTargetBuffer buffer, int length)
      throws IOException {
    Preconditions.checkArgument(length >= 0, "length should be non-negative");
    Preconditions.checkArgument(position >= 0, "position should be non-negative");
    if (length == 0) {
      return 0;
    }
    if (position >= mFileLength) { // at end of file
      return -1;
    }
    try (NettyDataReader reader = mReaderFactory.create(position, length)) {
      return reader.read(buffer);
    } catch (RuntimeException e) {
      UFS_FALLBACK_COUNTER.inc();
      LOG.debug("Dora client read file error ({} times). Fall back to UFS.",
          UFS_FALLBACK_COUNTER.getCount(), e);
      // TODO(lu) what if read partial failed, cleanup the buffer?
      return mExternalReader.get().positionRead(position, buffer, length);
    }
  }
}
