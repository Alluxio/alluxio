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

import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

/**
 * The interface represents a shadow cache which supports put/read/delete/aging operations.
 */
public interface ShadowCacheManager {

  /**
   * @param conf the Alluxio configuration
   * @return an instance of {@link ShadowCacheManager}
   */
  static ShadowCacheManager create(AlluxioConfiguration conf) {
    ShadowCacheType shadowCacheType =
        conf.getEnum(PropertyKey.USER_CLIENT_CACHE_SHADOW_TYPE, ShadowCacheType.class);
    switch (shadowCacheType) {
      case CLOCK_CUCKOO_FILTER:
        return new ClockCuckooShadowCacheManager(conf);
      case MULTIPLE_BLOOM_FILTER:
      default:
        return new MultipleBloomShadowCacheManager(conf);
    }
  }

  /**
   * Puts a page with specified size and scope into the shadow cache manager.
   *
   * @param pageId page identifier
   * @param size page size
   * @param scope cache scope
   * @return true if the put was successful, false otherwise
   */
  boolean put(PageId pageId, int size, CacheScope scope);

  /**
   * Reads the entire page and refresh its access time if the queried page is found in the cache.
   *
   * @param pageId page identifier
   * @param bytesToRead number of bytes to read in this page
   * @param scope cache scope
   * @return the number of bytes read, 0 if page is not found, -1 on errors
   */
  int get(PageId pageId, int bytesToRead, CacheScope scope);

  /**
   * Deletes a page from the cache.
   *
   * @param pageId page identifier
   * @return true if the page is successfully deleted, false otherwise
   */
  boolean delete(PageId pageId);

  /**
   * Aging all the pages stored in this shadow cache. Specifically, aging operation removes all the
   * stale pages which are not accessed for more than a sliding window.
   */
  void aging();

  /**
   * Update working set size in number of pages and bytes. Suggest calling this method before
   * getting the number of pages or bytes.
   */
  void updateWorkingSetSize();

  /**
   * Stop the background aging task.
   */
  void stopUpdate();

  /**
   * @return the number of pages in this shadow cache
   */
  long getShadowCachePages();

  /**
   * @param scope cache scope
   * @return the number of pages of given cache scope in this shadow cache
   */
  long getShadowCachePages(CacheScope scope);

  /**
   * @return the number of bytes in this shadow cache
   */
  long getShadowCacheBytes();

  /**
   * @param scope cache scope
   * @return the number of bytes of given cache scope in this shadow cache
   */
  long getShadowCacheBytes(CacheScope scope);

  /**
   * @return the number of pages read in this shadow cache
   */
  long getShadowCachePageRead();

  /**
   * @return the number of pages hit in this shadow cache
   */
  long getShadowCachePageHit();

  /**
   * @return the number of bytes read in this shadow cache
   */
  long getShadowCacheByteRead();

  /**
   * @return the number of bytes hit in this shadow cache
   */
  long getShadowCacheByteHit();

  /**
   * @return the false positive ratio
   */
  double getFalsePositiveRatio();
}
