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

package alluxio.client.file.cache;

import alluxio.client.file.CacheContext;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link FileInStream} that reads from a local cache if possible.
 */
@NotThreadSafe
public class LocalCacheFileInStream extends FileInStream {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheFileInStream.class);

  /** Page size in bytes. */
  protected final long mPageSize;

  private final Closer mCloser = Closer.create();

  /** Local store to store pages. */
  private final CacheManager mCacheManager;
  private final boolean mQuotaEnabled;
  /** Cache related context of this file. */
  private final CacheContext mCacheContext;
  /** File info, fetched from external FS. */
  private final URIStatus mStatus;
  private final FileInStreamOpener mExternalFileInStreamOpener;

  /** Stream reading from the external file system, opened once. */
  private FileInStream mExternalFileInStream;
  /** Current position of the stream, relative to the start of the file. */
  private long mPosition = 0;
  private boolean mClosed = false;
  private boolean mEOF = false;

  /**
   * Interface to wrap open method of file system.
   */
  public interface FileInStreamOpener {
    /**
     * Opens an FSDataInputStream at the indicated Path.
     *
     * @param uriStatus the file to open
     * @return input stream opened
     */
    FileInStream open(URIStatus uriStatus) throws IOException, AlluxioException;
  }

  /**
   * Constructor when the {@link URIStatus} is already available.
   *
   * @param status file status
   * @param fileOpener open file in the external file system if a cache miss occurs
   * @param cacheManager local cache manager
   * @param conf configuration
   */
  public LocalCacheFileInStream(URIStatus status, FileInStreamOpener fileOpener,
      CacheManager cacheManager, AlluxioConfiguration conf) {
    mPageSize = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    mExternalFileInStreamOpener = fileOpener;
    mCacheManager = cacheManager;
    mStatus = status;
    // Currently quota is only supported when it is set by external systems in status context
    mQuotaEnabled = conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED);
    if (mQuotaEnabled && status.getCacheContext() != null) {
      mCacheContext = status.getCacheContext();
    } else {
      mCacheContext = CacheContext.defaults();
    }
    Metrics.registerGauges();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return readInternal(b, off, len, ReadType.READ_INTO_BYTE_ARRAY, mPosition, false);
  }

  @Override
  public int read(ByteBuffer buf, int off, int len) throws IOException {
    byte[] b = new byte[buf.remaining()];
    int totalBytesRead =
        readInternal(b, off, len, ReadType.READ_INTO_BYTE_BUFFER, mPosition, false);
    if (totalBytesRead == -1) {
      return -1;
    }
    buf.put(b, off, totalBytesRead);
    return totalBytesRead;
  }

  // TODO(binfan): take ByteBuffer once CacheManager takes ByteBuffer to avoid extra mem copy
  private int readInternal(byte[] b, int off, int len, ReadType readType, long pos,
      boolean isPositionedRead) throws IOException {
    Preconditions.checkArgument(len >= 0, "length should be non-negative");
    Preconditions.checkArgument(off >= 0, "offset should be non-negative");
    Preconditions.checkArgument(pos >= 0, "position should be non-negative");
    if (len == 0) {
      return 0;
    }
    if (pos >= mStatus.getLength()) { // at end of file
      return -1;
    }
    int totalBytesRead = 0;
    long currentPosition = pos;
    long lengthToRead = Math.min(len, mStatus.getLength() - pos);
    // used in positionedRead, so make stopwatch a local variable rather than class member
    Stopwatch stopwatch = createUnstartedStopwatch();
    // for each page, check if it is available in the cache
    while (totalBytesRead < lengthToRead) {
      long currentPage = currentPosition / mPageSize;
      int currentPageOffset = (int) (currentPosition % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, lengthToRead - totalBytesRead);
      PageId pageId;
      CacheContext cacheContext = mStatus.getCacheContext();
      if (cacheContext != null && cacheContext.getCacheIdentifier() != null) {
        pageId = new PageId(cacheContext.getCacheIdentifier(), currentPage);
      } else {
        pageId = new PageId(Long.toString(mStatus.getFileId()), currentPage);
      }
      stopwatch.reset().start();
      int bytesRead =
          mCacheManager.get(pageId, currentPageOffset, bytesLeftInPage, b, off + totalBytesRead,
              mCacheContext);
      stopwatch.stop();
      if (bytesRead > 0) {
        totalBytesRead += bytesRead;
        currentPosition += bytesRead;
        Metrics.BYTES_READ_CACHE.mark(bytesRead);
        if (cacheContext != null) {
          cacheContext
              .incrementCounter(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getMetricName(), bytesRead);
          cacheContext.incrementCounter(
              MetricKey.CLIENT_CACHE_PAGE_READ_CACHE_TIME_NS.getMetricName(),
              stopwatch.elapsed(TimeUnit.NANOSECONDS));
        }
      } else {
        // on local cache miss, read a complete page from external storage. This will always make
        // progress or throw an exception
        stopwatch.reset().start();
        byte[] page = readExternalPage(currentPosition, readType);
        stopwatch.stop();
        if (page.length > 0) {
          System.arraycopy(page, currentPageOffset, b, off + totalBytesRead, bytesLeftInPage);
          totalBytesRead += bytesLeftInPage;
          currentPosition += bytesLeftInPage;
          Metrics.BYTES_REQUESTED_EXTERNAL.mark(bytesLeftInPage);
          if (cacheContext != null) {
            cacheContext.incrementCounter(
                MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getMetricName(), bytesLeftInPage);
            cacheContext.incrementCounter(
                MetricKey.CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS.getMetricName(),
                stopwatch.elapsed(TimeUnit.NANOSECONDS)
            );
          }
          mCacheManager.put(pageId, page, mCacheContext);
        }
      }
      if (!isPositionedRead) {
        mPosition = currentPosition;
      }
    }
    if (totalBytesRead > len || (totalBytesRead < len && currentPosition < mStatus.getLength())) {
      throw new IOException(String.format("Invalid number of bytes read - "
          + "bytes to read = %d, actual bytes read = %d, bytes remains in file %d",
          len, totalBytesRead, remaining()));
    }
    return totalBytesRead;
  }

  @VisibleForTesting
  protected Stopwatch createUnstartedStopwatch() {
    return Stopwatch.createUnstarted(Ticker.systemTicker());
  }

  @Override
  public long skip(long n) {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }
    long toSkip = Math.min(remaining(), n);
    mPosition += toSkip;
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  @Override
  public long remaining() {
    return mEOF ? 0 : mStatus.getLength() - mPosition;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return readInternal(b, off, len, ReadType.READ_INTO_BYTE_ARRAY, pos, true);
  }

  @Override
  public long getPos() {
    return mPosition;
  }

  @Override
  public void seek(long pos) {
    checkIfClosed();
    Preconditions.checkArgument(pos >= 0, "Seek position is negative: %s", pos);
    Preconditions
        .checkArgument(pos <= mStatus.getLength(),
            "Seek position (%s) exceeds the length of the file (%s)", pos, mStatus.getLength());
    if (pos == mPosition) {
      return;
    }
    if (pos < mPosition) {
      mEOF = false;
    }
    mPosition = pos;
  }

  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!mClosed, "Cannot operate on a closed stream");
  }

  /**
   * Convenience method to get external file reader with lazy init.
   * <p>
   * // TODO(calvin): Evaluate if using positioned read to allow for more concurrency is worthwhile
   *
   * @param pos position to set the external stream to
   */
  private FileInStream getExternalFileInStream(long pos) throws IOException {
    try {
      if (mExternalFileInStream == null) {
        mExternalFileInStream = mExternalFileInStreamOpener.open(mStatus);
        mCloser.register(mExternalFileInStream);
      }
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    long pageStart = pos - (pos % mPageSize);
    if (mExternalFileInStream.getPos() != pageStart) {
      mExternalFileInStream.seek(pageStart);
    }
    return mExternalFileInStream;
  }

  /**
   * Reads a page from external storage which contains the position specified. Note that this makes
   * a copy of the page.
   * <p>
   * This method is synchronized to ensure thread safety for positioned reads. Only a single thread
   * should call this method at a time because the underlying state (mExternalFileInStream) is
   * shared. Another way would be to use positioned reads instead of seek and read, but that assumes
   * the underlying FileInStream implements thread safe positioned reads which are not much more
   * expensive than seek and read.
   * <p>
   * TODO(calvin): Consider a more efficient API which does not require a data copy.
   *
   * @param pos the position which the page will contain
   * @return a byte array of the page data
   */
  private synchronized byte[] readExternalPage(long pos, ReadType readType) throws IOException {
    long pageStart = pos - (pos % mPageSize);
    FileInStream stream = getExternalFileInStream(pageStart);
    int pageSize = (int) Math.min(mPageSize, mStatus.getLength() - pageStart);
    byte[] page = new byte[pageSize];
    ByteBuffer buffer = readType == ReadType.READ_INTO_BYTE_BUFFER ? ByteBuffer.wrap(page) : null;
    int totalBytesRead = 0;
    while (totalBytesRead < pageSize) {
      int bytesRead;
      switch (readType) {
        case READ_INTO_BYTE_ARRAY:
          bytesRead = stream.read(page, totalBytesRead, pageSize - totalBytesRead);
          break;
        case READ_INTO_BYTE_BUFFER:
          bytesRead = stream.read(buffer);
          break;
        default:
          throw new IOException("unsupported read type = " + readType);
      }
      if (bytesRead <= 0) {
        break;
      }
      totalBytesRead += bytesRead;
    }
    Metrics.BYTES_READ_EXTERNAL.mark(totalBytesRead);
    if (totalBytesRead != pageSize) {
      throw new IOException("Failed to read complete page from external storage. Bytes read: "
          + totalBytesRead + " Page size: " + pageSize);
    }
    return page;
  }

  private static final class Metrics {
    /** Cache hits. */
    private static final Meter BYTES_READ_CACHE =
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getName());
    /** Bytes read from external, may be larger than requests due to reading complete pages. */
    private static final Meter BYTES_READ_EXTERNAL =
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL.getName());
    /** Cache misses. */
    private static final Meter BYTES_REQUESTED_EXTERNAL =
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getName());

    private static void registerGauges() {
      // Cache hit rate = Cache hits / (Cache hits + Cache misses).
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_HIT_RATE.getName()),
          () -> {
            long cacheHits = BYTES_READ_CACHE.getCount();
            long cacheMisses = BYTES_REQUESTED_EXTERNAL.getCount();
            long total = cacheHits + cacheMisses;
            if (total > 0) {
              return cacheHits / (1.0 * total);
            }
            return 0;
          });
    }
  }

  enum ReadType {
    /**
     * read some number of bytes from the input stream and stores them into the buffer array.
     */
    READ_INTO_BYTE_ARRAY,
    /**
     * read some number of bytes from the input stream and stores them into the ByteBuffer.
     */
    READ_INTO_BYTE_BUFFER
  }
}
