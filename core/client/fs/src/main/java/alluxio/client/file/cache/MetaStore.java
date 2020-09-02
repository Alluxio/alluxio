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
import alluxio.client.quota.PrestoCacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.PageNotFoundException;

/**
 * The metadata store for pages stored in cache.
 */
public interface MetaStore {

  /**
   * @param conf the alluxio configuration
   * @return an instance of MetaStore
   */
  static MetaStore create(AlluxioConfiguration conf) {
    return new DefaultMetaStore(conf);
  }

  /**
   * @param pageId page identifier
   * @return if a page is stored in cache
   */
  boolean hasPage(PageId pageId);

  /**
   * Adds a new page to the cache.
   *
   * @param pageId page identifier
   * @param pageInfo info of the page
   */
  void addPage(PageId pageId, PageInfo pageInfo);

  /**
   * @param pageId page identifier
   * @return page info
   */
  PageInfo getPageInfo(PageId pageId) throws PageNotFoundException;

  /**
   * Removes a page.
   *
   * @param pageId page identifier
   */
  void removePage(PageId pageId) throws PageNotFoundException;

  /**
   * @return the total size of pages stored in bytes
   */
  long bytes();

  /**
   * @param cacheScope scope to query
   * @return the total size of pages stored in bytes
   */
  long bytes(PrestoCacheScope cacheScope);

  /**
   * @return the number of pages stored
   */
  long pages();

  /**
   * Resets the meta store.
   */
  void reset();

  /**
   * @param cacheScope scope to evict
   * @return a page to evict
   */
  PageInfo evict(CacheScope cacheScope);
}
