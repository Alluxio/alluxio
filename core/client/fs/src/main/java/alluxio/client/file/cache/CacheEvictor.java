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

/**
 * Interface for client-side cache eviction policy.
 */
public interface CacheEvictor {

  /**
   * @return a CacheEvictor instance
   */
  static CacheEvictor create() {
    // return corresponding CacheEvictor impl
    return new LRUCacheEvictor();
  }

  /**
   * Updates evictor after a get operation.
   *
   * @param pageId page identifier
   */
  void updateOnGet(PageId pageId);

  /**
   * Updates evictor after a put operation.
   *
   * @param pageId page identifier
   */
  void updateOnPut(PageId pageId);

  /**
   * Updates evictor after a delete operation.
   *
   * @param pageId page identifier
   */
  void updateOnDelete(PageId pageId);

  /**
   * Find a page to evict.
   *
   * @return identifier of the page to evict
   */
  PageId evict();
}
