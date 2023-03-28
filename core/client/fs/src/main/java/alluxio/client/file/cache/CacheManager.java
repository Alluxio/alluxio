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
import alluxio.client.file.cache.store.ByteArrayTargetBuffer;
import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import javax.annotation.concurrent.GuardedBy;

/**
 * Interface for managing cached pages.
 */
public interface CacheManager extends AutoCloseable, CacheStatus {
  Logger LOG = LoggerFactory.getLogger(CacheManager.class);

  /**
   * State of a cache.
   */
  enum State {
    /**
     * this cache is not in use.
     */
    NOT_IN_USE(0),
    /**
     * this cache is read only.
     */
    READ_ONLY(1),
    /**
     * this cache can both read and write.
     */
    READ_WRITE(2);

    private final int mValue;

    State(int value) {
      mValue = value;
    }

    /**
     * @return the value of the state
     */
    public int getValue() {
      return mValue;
    }
  }

  /**
   * Factory class to get or create a CacheManager.
   */
  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Factory.class);
    private static final Lock CACHE_INIT_LOCK = new ReentrantLock();
    @GuardedBy("CACHE_INIT_LOCK")
    private static final AtomicReference<CacheManager> CACHE_MANAGER = new AtomicReference<>();

    /**
     * @param conf the Alluxio configuration
     * @return current CacheManager handle, creating a new one if it doesn't yet exist or null in
     *         case creation takes a long time by other threads.
     */
    public static CacheManager get(AlluxioConfiguration conf) throws IOException {
      // TODO(feng): support multiple cache managers
      if (CACHE_MANAGER.get() == null) {
        try (LockResource lockResource = new LockResource(CACHE_INIT_LOCK)) {
          if (CACHE_MANAGER.get() == null) {
            CACHE_MANAGER.set(
                create(conf));
          }
        } catch (IOException e) {
          Metrics.CREATE_ERRORS.inc();
          throw new IOException("Failed to create CacheManager", e);
        }
      }
      return CACHE_MANAGER.get();
    }

    /**
     * @param conf the Alluxio configuration
     * @return an instance of {@link CacheManager}
     */
    public static CacheManager create(AlluxioConfiguration conf) throws IOException {
      CacheManagerOptions options = CacheManagerOptions.create(conf);
      return create(conf, options, PageMetaStore.create(options));
    }

    /**
     * @param conf the Alluxio configuration
     * @param options the options for local cache manager
     * @param pageMetaStore  meta store for pages
     * @return an instance of {@link CacheManager}
     */
    public static CacheManager create(AlluxioConfiguration conf,
        CacheManagerOptions options, PageMetaStore pageMetaStore) throws IOException {
      try {
        boolean isShadowCacheEnabled =
            conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_SHADOW_ENABLED);
        boolean isNettyDataTransmissionEnable =
            conf.getBoolean(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED);
        // Note that Netty data transmission doesn't support async write
        if (isNettyDataTransmissionEnable) {
          options.setIsAsyncWriteEnabled(false);
        }
        if (isShadowCacheEnabled) {
          return new NoExceptionCacheManager(
              new CacheManagerWithShadowCache(LocalCacheManager.create(options, pageMetaStore),
                  conf));
        }
        return new NoExceptionCacheManager(LocalCacheManager.create(options, pageMetaStore));
      } catch (IOException e) {
        Metrics.CREATE_ERRORS.inc();
        LOG.error("Failed to create CacheManager", e);
        throw e;
      }
    }

    /**
     * Removes the current {@link CacheManager} if it exists.
     */
    static void clear() {
      try (LockResource r = new LockResource(CACHE_INIT_LOCK)) {
        CacheManager manager = CACHE_MANAGER.getAndSet(null);
        if (manager != null) {
          manager.close();
        }
      } catch (Exception e) {
        LOG.warn("Failed to close CacheManager: {}", e.toString());
      }
    }

    private Factory() {} // prevent instantiation

    private static final class Metrics {
      // Note that only counter can be added here.
      // Both meter and timer need to be used inline
      // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}
      /** Errors when creating cache. */
      private static final Counter CREATE_ERRORS =
          MetricsSystem.counter(MetricKey.CLIENT_CACHE_CREATE_ERRORS.getName());

      private Metrics() {} // prevent instantiation
    }
  }

  /**
   * Puts a page into the cache manager. This method is best effort. It is possible that this put
   * operation returns without page written.
   *
   * @param pageId page identifier
   * @param page page data
   * @return true if the put was successful, false otherwise
   */
  default boolean put(PageId pageId, byte[] page) {
    return put(pageId, page, CacheContext.defaults());
  }

  /**
   * Puts a page into the cache manager. This method is best effort. It is possible that this put
   * operation returns without page written.
   *
   * @param pageId page identifier
   * @param page page data
   * @return true if the put was successful, false otherwise
   */
  default boolean put(PageId pageId, ByteBuffer page) {
    return put(pageId, page, CacheContext.defaults());
  }

  /**
   * Puts a page into the cache manager with scope and quota respected. This method is best effort.
   * It is possible that this put operation returns without page written.
   *
   * @param pageId page identifier
   * @param page page data
   * @param cacheContext cache related context
   * @return true if the put was successful, false otherwise
   */
  default boolean put(PageId pageId, byte[] page, CacheContext cacheContext) {
    return put(pageId, ByteBuffer.wrap(page), cacheContext);
  }

  /**
   * Puts a page into the cache manager with scope and quota respected. This method is best effort.
   * It is possible that this put operation returns without page written.
   *
   * @param pageId page identifier
   * @param page page data
   * @param cacheContext cache related context
   * @return true if the put was successful, false otherwise
   */
  boolean put(PageId pageId, ByteBuffer page, CacheContext cacheContext);

  /**
   * Reads the entire page if the queried page is found in the cache, stores the result in buffer.
   *
   * @param pageId page identifier
   * @param bytesToRead number of bytes to read in this page
   * @param buffer destination buffer to write
   * @param offsetInBuffer offset in the destination buffer to write
   * @return number of bytes read, 0 if page is not found, -1 on errors
   */
  default int get(PageId pageId, int bytesToRead, byte[] buffer, int offsetInBuffer) {
    return get(pageId, 0, bytesToRead, buffer, offsetInBuffer);
  }

  /**
   * Reads a part of a page if the queried page is found in the cache, stores the result in buffer.
   *
   * @param pageId page identifier
   * @param pageOffset offset into the page
   * @param bytesToRead number of bytes to read in this page
   * @param buffer destination buffer to write
   * @param offsetInBuffer offset in the destination buffer to write
   * @return number of bytes read, 0 if page is not found, -1 on errors
   */
  default int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer,
      int offsetInBuffer) {
    return get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer, CacheContext.defaults());
  }

  /**
   * Reads a part of a page if the queried page is found in the cache, stores the result in buffer.
   *
   * @param pageId page identifier
   * @param pageOffset offset into the page
   * @param bytesToRead number of bytes to read in this page
   * @param buffer destination buffer to write
   * @param offsetInBuffer offset in the destination buffer to write
   * @param cacheContext cache related context
   * @return number of bytes read, 0 if page is not found, -1 on errors
   */
  default int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int offsetInBuffer,
      CacheContext cacheContext) {
    return get(pageId, pageOffset, bytesToRead, new ByteArrayTargetBuffer(buffer, offsetInBuffer),
        cacheContext);
  }

  /**
   * Reads a part of a page if the queried page is found in the cache, stores the result in buffer.
   *
   * @param pageId page identifier
   * @param pageOffset offset into the page
   * @param bytesToRead number of bytes to read in this page
   * @param buffer destination buffer to write
   * @param cacheContext cache related context
   * @return number of bytes read, 0 if page is not found, -1 on errors
   */
  int get(PageId pageId, int pageOffset, int bytesToRead, PageReadTargetBuffer buffer,
      CacheContext cacheContext);

  /**
   * Reads a part of a page with metrics recording.
   *
   * @param pageId page identifier
   * @param pageOffset offset into the page
   * @param bytesToRead number of bytes to read in this page
   * @param buffer destination buffer to write
   * @param cacheContext cache related context
   * @param stopwatch stop watch to record metrics
   * @return number of bytes read, 0 if page is not found, -1 on errors
   */
  default int getWithMetrics(PageId pageId, int pageOffset, int bytesToRead,
      PageReadTargetBuffer buffer, CacheContext cacheContext, Stopwatch stopwatch) {
    stopwatch.reset().start();
    int bytesRead =
        get(pageId, pageOffset, bytesToRead, buffer, cacheContext);
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
    }
    return bytesRead;
  }

  /**
   * Reads a part of a page if the queried page is found in the cache, stores the result in buffer.
   * Loads the page otherwise.
   *
   * @param pageId page identifier
   * @param pageOffset offset into the page
   * @param bytesToRead number of bytes to read in this page
   * @param buffer destination buffer to write
   * @param cacheContext cache related context
   * @param reader the external page reader to get page
   * @return number of bytes read, 0 if page is not found, -1 on errors
   */
  default int getAndLoad(PageId pageId, int pageOffset, int bytesToRead,
      PageReadTargetBuffer buffer, CacheContext cacheContext, ExternalPageReader reader) {
    Stopwatch stopwatch = createUnstartedStopwatch();
    int bytesRead = getWithMetrics(pageId, pageOffset,
        bytesToRead, buffer, cacheContext, stopwatch);
    if (bytesRead > 0) {
      return bytesRead;
    }
    byte[] page;
    try {
      stopwatch.reset().start();
      page = reader.read();
      stopwatch.stop();
    } catch (IOException e) {
      LOG.error("Failed to load external page", e);
      throw new RuntimeException(e);
    }
    if (page.length == 0) {
      return 0;
    }
    buffer.writeBytes(page, pageOffset, bytesToRead);
    MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getName())
        .mark(bytesToRead);
    if (cacheContext != null) {
      cacheContext.incrementCounter(
          MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getMetricName(), BYTE,
          bytesToRead);
      cacheContext.incrementCounter(
          MetricKey.CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS.getMetricName(), NANO,
          stopwatch.elapsed(TimeUnit.NANOSECONDS)
      );
      put(pageId, page, cacheContext);
    }
    return bytesToRead;
  }

  /**
   * Get page ids by the given file id.
   * @param fileId file identifier
   * @param fileLength file length (this will not be needed after we have per-file metadata)
   * @return a list of page ids which belongs to the file
   */
  default List<PageId> getCachedPageIdsByFileId(String fileId, long fileLength) {
    throw new UnsupportedOperationException();
  }

  /**
   * Deletes a page from the cache.
   *
   * @param pageId page identifier
   * @return true if the page is successfully deleted, false otherwise
   */
  boolean delete(PageId pageId);

  /**
   * @return state of this cache
   */
  State state();

  /**
   *
   * @param pageId
   * @param appendAt
   * @param page
   * @param cacheContext
   * @return true if append was successful
   */
  boolean append(PageId pageId, int appendAt, byte[] page, CacheContext cacheContext);

  /**
   * Invalidate the pages that match the given predicate.
   * @param predicate
   */
  default void invalidate(Predicate<PageInfo> predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  Optional<CacheUsage> getUsage();

  /**
   * @return the stop watch
   */
  @VisibleForTesting
  default Stopwatch createUnstartedStopwatch() {
    return Stopwatch.createUnstarted(Ticker.systemTicker());
  }

  /**
   * Interface to wrap external page reader which should be thread-safe.
   */
  interface ExternalPageReader {
    /**
     * Reads a page from external data reader.
     *
     * @return the bytes read
     */
    byte[] read() throws IOException;
  }
}
