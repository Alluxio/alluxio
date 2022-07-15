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
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

/**
 * Interface for managing cached pages.
 */
public interface CacheManager extends AutoCloseable {

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
                create(conf, PageMetaStore.create(conf), PageStoreDir.createPageStoreDirs(conf)));
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
     * @param pageMetaStore meta store for pages
     * @param dirs directories for local cache
     * @return an instance of {@link CacheManager}
     */
    public static CacheManager create(AlluxioConfiguration conf,
        PageMetaStore pageMetaStore,
        List<PageStoreDir> dirs) throws IOException {
      try {
        boolean isShadowCacheEnabled =
            conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_SHADOW_ENABLED);

        if (isShadowCacheEnabled) {
          return new NoExceptionCacheManager(
              new CacheManagerWithShadowCache(LocalCacheManager.create(conf, pageMetaStore, dirs),
                  conf));
        }
        return new NoExceptionCacheManager(LocalCacheManager.create(conf, pageMetaStore, dirs));
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
   * Puts a page into the cache manager with scope and quota respected. This method is best effort.
   * It is possible that this put operation returns without page written.
   *
   * @param pageId page identifier
   * @param page page data
   * @param cacheContext cache related context
   * @return true if the put was successful, false otherwise
   */
  boolean put(PageId pageId, byte[] page, CacheContext cacheContext);

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
  int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int offsetInBuffer,
      CacheContext cacheContext);

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
}
