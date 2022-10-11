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

import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * The metadata store for pages stored in cache.
 */
public interface PageMetaStore {

  /**
   * @param conf the alluxio configuration
   * @return an instance of MetaStore
   */
  static PageMetaStore create(AlluxioConfiguration conf) throws IOException {
    List<PageStoreDir> dirs = PageStoreDir.createPageStoreDirs(conf);
    if (conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED)) {
      return new QuotaPageMetaStore(conf, dirs);
    }
    return new DefaultPageMetaStore(dirs);
  }

  /**
   * @return the associated lock
   */
  ReadWriteLock getLock();

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
   * Adds a new temp page to the cache.
   *
   * @param pageId page identifier
   * @param pageInfo info of the page
   */
  void addTempPage(PageId pageId, PageInfo pageInfo);

  /**
   * Commits a temp file so that all its pages become permanent.
   *
   * @param fileId the temp file to commit
   * @param newFileId the new file name of the file after committing
   */
  void commitFile(String fileId, String newFileId) throws PageNotFoundException;

  /**
   * Gets the storage directories.
   *
   * @return the storage directories
   */
  List<PageStoreDir> getStoreDirs();

  /**
   * @param fileId
   * @param fileLength
   * @return the storage directory
   */
  PageStoreDir allocate(String fileId, long fileLength);

  /**
   * @param pageId page identifier
   * @return page info
   */
  PageInfo getPageInfo(PageId pageId) throws PageNotFoundException;

  /**
   * Removes a page.
   *
   * @param pageId page identifier
   * @return page info removed
   */
  PageInfo removePage(PageId pageId) throws PageNotFoundException;

  /**
   * @return the total size of pages stored in bytes
   */
  long bytes();

  /**
   * @return the number of pages stored
   */
  long numPages();

  /**
   * Resets the meta store.
   */
  void reset();

  /**
   * @param pageStoreDir
   * @return a page to evict
   */
  default PageInfo evict(PageStoreDir pageStoreDir) {
    return evict(CacheScope.GLOBAL, pageStoreDir);
  }

  /**
   * @param cacheScope
   * @param pageStoreDir
   * @return a page to evict
   */
  PageInfo evict(CacheScope cacheScope, PageStoreDir pageStoreDir);
}
