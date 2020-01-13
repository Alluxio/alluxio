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
public class MetaStore {

  /**
   * Default constructor.
   */
  public MetaStore() {
  }

  /**
   * @param pageId page identifier
   * @return if a page is stored in cache
   */
  boolean hasPage(PageId pageId) {
    return true;
  }

  /**
   * Adds a new page to the cache.
   *
   * @param pageId page identifier
   * @return true if added successfully
   */
  boolean addPage(PageId pageId) {
    return true;
  }

  /**
   * Removes a page.
   *
   * @param pageId page identifier
   */
  void removePage(PageId pageId) throws PageNotFoundException {
  }
}
