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

import alluxio.exception.PageNotFoundException;

/**
 * The metadata store for pages stored in cache.
 */
public interface MetaStore {

  /**
   * @return an instance of MetaStore
   */
  static MetaStore create() {
    return new DefaultMetaStore();
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
   * @param pageSize page size in bytes
   * @return true if added successfully
   */
  void addPage(PageId pageId, long pageSize);

  /**
   * Adds a new page to the cache.
   *
   * @param pageId page identifier
   * @return page size in bytes
   */
  long getSize(PageId pageId) throws PageNotFoundException;

  /**
   * Removes a page.
   *
   * @param pageId page identifier
   */
  void removePage(PageId pageId) throws PageNotFoundException;
}
