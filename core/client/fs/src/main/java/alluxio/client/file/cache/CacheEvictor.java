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

import alluxio.collections.Pair;

/**
 * Interface for client-side cache eviction policy.
 */
public interface CacheEvictor {

  /**
   * @return a CacheEvictor instance
   */
  static CacheEvictor create() {
    // return corresponding CacheEvictor impl
    return null;
  }

  /**
   * Updates evictor after a get operation.
   *
   * @param fileId ID of the file
   * @param pageIndex index of the page within the file
   */
  void updateOnGet(long fileId, long pageIndex);

  /**
   * Updates evictor after a put operation.
   *
   * @param fileId ID of the file
   * @param pageIndex index of the page within the file
   */
  void updateOnPut(long fileId, long pageIndex);

  /**
   * Updates evictor after a delete operation.
   *
   * @param fileId ID of the file
   * @param pageIndex index of the page within the file
   */
  void updateOnDelete(long fileId, long pageIndex);

  /**
   * Find a page to evict.
   *
   * @return a pair of long values representing (fileId, pageIndex)
   */
  Pair<Long, Long> evict();
}
