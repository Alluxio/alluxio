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

import alluxio.conf.AlluxioConfiguration;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Interface for managing cached pages.
 */
public interface CacheManager extends AutoCloseable  {
  /**
   * Factory class to get or create a CacheManager.
   */
  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Factory.class);
    private static CacheManager sCacheManager = null;

    /**
     * @param conf the Alluxio configuration
     * @return current CacheManager, creating a new one if it doesn't yet exist
     */
    public static synchronized CacheManager get(AlluxioConfiguration conf) throws IOException {
      // TODO(feng): support multiple cache managers
      if (sCacheManager == null) {
        sCacheManager = create(conf);
      }
      return sCacheManager;
    }

    /**
     * @param conf the Alluxio configuration
     * @return an instance of {@link CacheManager}
     */
    static CacheManager create(AlluxioConfiguration conf) throws IOException {
      try {
        return new NoExceptionCacheManager(LocalCacheManager.create(conf));
      } catch (IOException e) {
        Metrics.CREATE_ERRORS.inc();
        LOG.error("Failed to create CacheManager", e);
        throw e;
      }
    }

    private Factory() {} // prevent instantiation

    private static final class Metrics {
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
  boolean put(PageId pageId, byte[] page);

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
   * Reads a part of a page if the queried page is found in the cache, stores the result in
   * buffer.
   *
   * @param pageId page identifier
   * @param pageOffset offset into the page
   * @param bytesToRead number of bytes to read in this page
   * @param buffer destination buffer to write
   * @param offsetInBuffer offset in the destination buffer to write
   * @return number of bytes read, 0 if page is not found, -1 on errors
   */
  int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int offsetInBuffer);

  /**
   * Deletes a page from the cache.
   *
   * @param pageId page identifier
   * @return true if the page is successfully deleted, false otherwise
   */
  boolean delete(PageId pageId);
}
