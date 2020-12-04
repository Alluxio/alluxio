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

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.io.BufferUtils;

import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link FileInStream} that reads from a local cache if possible.
 */
@NotThreadSafe
public class LocalCacheFileInStream extends FileInStream {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheFileInStream.class);

  /** Page size in bytes. */
  protected final long mPageSize;

  private final byte[] mSingleByte = new byte[1];
  private final Closer mCloser = Closer.create();

  /** Local store to store pages. */
  private final CacheManager mCacheManager;
  /** Path of the file. */
  private final AlluxioURI mPath;
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
    mPath = new AlluxioURI(status.getPath());
    mExternalFileInStreamOpener = fileOpener;
    mCacheManager = cacheManager;
    mStatus = status;
    Metrics.registerGauges();
  }

  @Override
  public int read() throws IOException {
    int bytesRead = read(mSingleByte);
    if (bytesRead == -1) {
      return -1;
    }
    Preconditions.checkState(bytesRead == 1);
    return BufferUtils.byteToInt(mSingleByte[0]);
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(len >= 0, "length should be non-negative");
    Preconditions.checkArgument(off >= 0, "offset should be non-negative");
    if (len == 0) {
      return 0;
    }
    if (mPosition >= mStatus.getLength()) { // at end of file
      return -1;
    }
    int totalBytesRead = 0;
    long lengthToRead = Math.min(len, mStatus.getLength() - mPosition);
    // for each page, check if it is available in the cache
    while (totalBytesRead < lengthToRead) {
      long currentPage = mPosition / mPageSize;
      int currentPageOffset = (int) (mPosition % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, lengthToRead - totalBytesRead);
      PageId pageId = new PageId(mStatus.getFileIdentifier(), currentPage);
      int bytesRead =
          mCacheManager.get(pageId, currentPageOffset, bytesLeftInPage, b, off + totalBytesRead);
      if (bytesRead > 0) {
        totalBytesRead += bytesRead;
        mPosition += bytesRead;
        Metrics.BYTES_READ_CACHE.mark(bytesRead);
      } else {
        // on local cache miss, read a complete page from external storage. This will always make
        // progress or throw an exception
        byte[] page = readExternalPage(mPosition);
        if (page.length > 0) {
          System.arraycopy(page, currentPageOffset, b, off + totalBytesRead, bytesLeftInPage);
          totalBytesRead += bytesLeftInPage;
          mPosition += bytesLeftInPage;
          Metrics.BYTES_REQUESTED_EXTERNAL.mark(bytesLeftInPage);
          mCacheManager.put(pageId, page);
        }
      }
    }
    if (totalBytesRead > len || (totalBytesRead < len && remaining() > 0)) {
      throw new IOException(String.format("Invalid number of bytes read - "
          + "bytes to read = %d, actual bytes read = %d, bytes remains in file %d",
          len, totalBytesRead, remaining()));
    }
    return totalBytesRead;
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
    Preconditions.checkArgument(len >= 0, "length should be non-negative");
    Preconditions.checkArgument(off >= 0, "offset should be non-negative");
    Preconditions.checkArgument(pos >= 0, "position should be non-negative");
    if (len == 0) {
      return 0;
    }
    if (pos < 0 || pos >= mStatus.getLength()) { // at end of file
      return -1;
    }
    int totalBytesRead = 0;
    long currentPosition = pos;
    long lengthToRead = Math.min(len, mStatus.getLength() - pos);
    // for each page, check if it is available in the cache
    while (totalBytesRead < lengthToRead) {
      long currentPage = currentPosition / mPageSize;
      int currentPageOffset = (int) (currentPosition % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, lengthToRead - totalBytesRead);
      PageId pageId = new PageId(mStatus.getFileIdentifier(), currentPage);
      int bytesRead =
          mCacheManager.get(pageId, currentPageOffset, bytesLeftInPage, b, off + totalBytesRead);
      if (bytesRead > 0) {
        totalBytesRead += bytesRead;
        currentPosition += bytesRead;
        Metrics.BYTES_READ_CACHE.mark(bytesRead);
      } else {
        // on local cache miss, read a complete page from external storage. This will always make
        // progress or throw an exception
        byte[] page = readExternalPage(currentPosition);
        if (page.length > 0) {
          System.arraycopy(page, currentPageOffset, b, off + totalBytesRead, bytesLeftInPage);
          totalBytesRead += bytesLeftInPage;
          currentPosition += bytesLeftInPage;
          Metrics.BYTES_REQUESTED_EXTERNAL.mark(bytesLeftInPage);
          mCacheManager.put(pageId, page);
        }
      }
    }
    if (totalBytesRead > len || (totalBytesRead < len && currentPosition < mStatus.getLength())) {
      throw new IOException(String.format(
          "Invalid number of bytes positionread - read from position = %d, "
              + "bytes to read = %d, actual bytes read = %d, bytes remains in file %d",
          pos, len, totalBytesRead, mStatus.getLength() - currentPosition)
      );
    }
    return totalBytesRead;
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
   *
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
   *
   * This method is synchronized to ensure thread safety for positioned reads. Only a single thread
   * should call this method at a time because the underlying state (mExternalFileInStream) is
   * shared. Another way would be to use positioned reads instead of seek and read, but that assumes
   * the underlying FileInStream implements thread safe positioned reads which are not much more
   * expensive than seek and read.
   *
   * TODO(calvin): Consider a more efficient API which does not require a data copy.
   *
   * @param pos the position which the page will contain
   * @return a byte array of the page data
   */
  private synchronized byte[] readExternalPage(long pos) throws IOException {
    long pageStart = pos - (pos % mPageSize);
    FileInStream stream = getExternalFileInStream(pageStart);
    int pageSize = (int) Math.min(mPageSize, mStatus.getLength() - pageStart);
    byte[] page = new byte[pageSize];
    int totalBytesRead = 0;
    while (totalBytesRead < pageSize) {
      int bytesRead = stream.read(page, totalBytesRead, pageSize - totalBytesRead);
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
}
