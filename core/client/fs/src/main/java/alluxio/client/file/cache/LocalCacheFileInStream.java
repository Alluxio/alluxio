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

import static alluxio.client.file.CacheContext.StatsUnit.BYTE;
import static alluxio.client.file.CacheContext.StatsUnit.NANO;

import alluxio.client.file.CacheContext;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.store.ByteArrayTargetBuffer;
import alluxio.client.file.cache.store.ByteBufferTargetBuffer;
import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

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
  private final int mBufferSize;

  private byte[] mBuffer = null;
  private long mBufferStartOffset;
  private long mBufferEndOffset;

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
   * Registers metrics.
   */
  public static void registerMetrics() {
    Metrics.registerGauges();
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

    mBufferSize = (int) conf.getBytes(PropertyKey.USER_CLIENT_CACHE_IN_STREAM_BUFFER_SIZE);
    Preconditions.checkArgument(mBufferSize >= 0, "Buffer size cannot be negative. %s", mPageSize);
    if (mBufferSize > 0) {
      mBuffer = new byte[mBufferSize];
    }
  }

  @Override
  public int read(byte[] bytesBuffer, int offset, int length) throws IOException {
    return readInternal(new ByteArrayTargetBuffer(bytesBuffer, offset), offset, length,
        ReadType.READ_INTO_BYTE_ARRAY, mPosition, false);
  }

  @Override
  public int read(ByteBuffer buffer, int offset, int length) throws IOException {
    int totalBytesRead = readInternal(new ByteBufferTargetBuffer(buffer), offset, length,
        ReadType.READ_INTO_BYTE_BUFFER, mPosition, false);
    if (totalBytesRead == -1) {
      return -1;
    }
    return totalBytesRead;
  }

  private int bufferedRead(PageReadTargetBuffer targetBuffer, int length,
      ReadType readType, long position, Stopwatch stopwatch) throws IOException {
    if (mBuffer == null) { //buffer is disabled, read data from local cache directly.
      return localCachedRead(targetBuffer, length, readType, position, stopwatch);
    }
    //hit or partially hit the in stream buffer
    if (position > mBufferStartOffset && position < mBufferEndOffset) {
      int lengthToReadFromBuffer = (int) Math.min(length,
          mBufferEndOffset - position);
      targetBuffer.writeBytes(mBuffer, (int) (position - mBufferStartOffset),
          lengthToReadFromBuffer);
      return lengthToReadFromBuffer;
    }
    if (length >= mBufferSize) {
      // Skip load to the in stream buffer if the data piece is larger than buffer size
      return localCachedRead(targetBuffer, length, readType, position, stopwatch);
    }
    int bytesLoadToBuffer = (int) Math.min(mBufferSize, mStatus.getLength() - position);
    int bytesRead =
        localCachedRead(new ByteArrayTargetBuffer(mBuffer, 0),  bytesLoadToBuffer, readType,
            position, stopwatch);
    mBufferStartOffset = position;
    mBufferEndOffset = position + bytesRead;
    int dataReadFromBuffer = Math.min(bytesRead, length);
    targetBuffer.writeBytes(mBuffer, 0, dataReadFromBuffer);
    MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_IN_STREAM_BUFFER.getName())
        .mark(dataReadFromBuffer);
    return dataReadFromBuffer;
  }

  private int localCachedRead(PageReadTargetBuffer bytesBuffer, int length,
      ReadType readType, long position, Stopwatch stopwatch) throws IOException {
    long currentPage = position / mPageSize;
    PageId pageId;
    CacheContext cacheContext = mStatus.getCacheContext();
    if (cacheContext != null && cacheContext.getCacheIdentifier() != null) {
      pageId = new PageId(cacheContext.getCacheIdentifier(), currentPage);
    } else {
      pageId = new PageId(Long.toString(mStatus.getFileId()), currentPage);
    }
    int currentPageOffset = (int) (position % mPageSize);
    int bytesLeftInPage = (int) (mPageSize - currentPageOffset);
    int bytesToReadInPage = Math.min(bytesLeftInPage, length);
    stopwatch.reset().start();
    int bytesRead =
        mCacheManager.get(pageId, currentPageOffset, bytesToReadInPage, bytesBuffer, mCacheContext);
    stopwatch.stop();
    if (bytesRead > 0) {
      MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getName()).mark(bytesRead);
      if (cacheContext != null) {
        cacheContext.incrementCounter(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getMetricName(), BYTE,
            bytesRead);
        cacheContext.incrementCounter(
            MetricKey.CLIENT_CACHE_PAGE_READ_CACHE_TIME_NS.getMetricName(), NANO,
            stopwatch.elapsed(TimeUnit.NANOSECONDS));
      }
      return bytesRead;
    }
    // on local cache miss, read a complete page from external storage. This will always make
    // progress or throw an exception
    stopwatch.reset().start();
    byte[] tmpPage = mCacheManager.acquire(pageId, mPageSize);
    if (tmpPage == null) {
      long pageStart = position - (position % mPageSize);
      int sizeToRead = (int) Math.min(mPageSize, mStatus.getLength() - pageStart);
      tmpPage = new byte[sizeToRead];
    }
    int byteRead = readExternalPage(tmpPage, position, readType);
    stopwatch.stop();
    if (byteRead > 0) {
      bytesBuffer.writeBytes(tmpPage, currentPageOffset, bytesToReadInPage);
      // cache misses
      MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getName())
          .mark(bytesToReadInPage);
      if (cacheContext != null) {
        cacheContext.incrementCounter(
            MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getMetricName(), BYTE,
            bytesToReadInPage);
        cacheContext.incrementCounter(
            MetricKey.CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS.getMetricName(), NANO,
            stopwatch.elapsed(TimeUnit.NANOSECONDS)
        );
      }
      mCacheManager.put(pageId, ByteBuffer.wrap(tmpPage, 0, byteRead), mCacheContext);
    }
    return bytesToReadInPage;
  }

  // TODO(binfan): take ByteBuffer once CacheManager takes ByteBuffer to avoid extra mem copy
  private int readInternal(PageReadTargetBuffer targetBuffer, int offset, int length,
      ReadType readType, long position, boolean isPositionedRead) throws IOException {
    Preconditions.checkArgument(length >= 0, "length should be non-negative");
    Preconditions.checkArgument(offset >= 0, "offset should be non-negative");
    Preconditions.checkArgument(position >= 0, "position should be non-negative");
    if (length == 0) {
      return 0;
    }
    if (position >= mStatus.getLength()) { // at end of file
      return -1;
    }
    int totalBytesRead = 0;
    long currentPosition = position;
    long lengthToRead = Math.min(length, mStatus.getLength() - position);
    // used in positionedRead, so make stopwatch a local variable rather than class member
    Stopwatch stopwatch = createUnstartedStopwatch();
    // for each page, check if it is available in the cache
    while (totalBytesRead < lengthToRead) {
      int bytesRead = bufferedRead(targetBuffer,
          (int) (lengthToRead - totalBytesRead), readType, currentPosition, stopwatch);
      totalBytesRead += bytesRead;
      currentPosition += bytesRead;
      if (!isPositionedRead) {
        mPosition = currentPosition;
      }
    }
    if (totalBytesRead > length
        || (totalBytesRead < length && currentPosition < mStatus.getLength())) {
      throw new IOException(String.format("Invalid number of bytes read - "
          + "bytes to read = %d, actual bytes read = %d, bytes remains in file %d",
          length, totalBytesRead, remaining()));
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
    return readInternal(new ByteArrayTargetBuffer(b, off), off, len, ReadType.READ_INTO_BYTE_ARRAY,
        pos, true);
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
   * @param position position to set the external stream to
   */
  private FileInStream getExternalFileInStream(long position) throws IOException {
    try {
      if (mExternalFileInStream == null) {
        mExternalFileInStream = mExternalFileInStreamOpener.open(mStatus);
        mCloser.register(mExternalFileInStream);
      }
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    long pageStart = position - (position % mPageSize);
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
   * @param position the position which the page will contain
   * @return a byte array of the page data
   */
  private synchronized int readExternalPage(byte[] page, long position, ReadType readType)
      throws IOException {
    long pageStart = position - (position % mPageSize);
    FileInStream stream = getExternalFileInStream(pageStart);
    int sizeToRead = (int) Math.min(mPageSize, mStatus.getLength() - pageStart);

    ByteBuffer buffer = readType == ReadType.READ_INTO_BYTE_BUFFER ? ByteBuffer.wrap(page) : null;
    int totalBytesRead = 0;
    while (totalBytesRead < sizeToRead) {
      int bytesRead;
      switch (readType) {
        case READ_INTO_BYTE_ARRAY:
          bytesRead = stream.read(page, totalBytesRead, sizeToRead - totalBytesRead);
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
    // Bytes read from external, may be larger than requests due to reading complete pages
    MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL.getName()).mark(totalBytesRead);
    if (totalBytesRead != sizeToRead) {
      throw new IOException("Failed to read complete page from external storage. Bytes read: "
          + totalBytesRead + " Page size: " + sizeToRead);
    }
    return totalBytesRead;
  }

  private static final class Metrics {
    // Note that only counter/guage can be added here.
    // Both meter and timer need to be used inline
    // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}

    private static void registerGauges() {
      // Cache hit rate = Cache hits / (Cache hits + Cache misses).
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_HIT_RATE.getName()),
          () -> {
            long cacheHits = MetricsSystem.meter(
                MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getName()).getCount();
            long cacheMisses = MetricsSystem.meter(
                MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getName()).getCount();
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
