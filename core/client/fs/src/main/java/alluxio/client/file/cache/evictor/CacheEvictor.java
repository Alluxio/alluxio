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

package alluxio.client.file.cache.evictor;

import alluxio.client.file.cache.PageId;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Interface for client-side cache eviction policy. The implementation of this evictor must be
 * thread-safe.
 */
@ThreadSafe
public interface CacheEvictor {

  /**
   * @param conf the alluxio configuration
   * @return a CacheEvictor instance
   */
  static CacheEvictor create(AlluxioConfiguration conf) {
    boolean isNondeterministic =
        conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_EVICTOR_NONDETERMINISTIC_ENABLED);
    boolean isLRU =
        conf.getClass(PropertyKey.USER_CLIENT_CACHE_EVICTOR_CLASS).equals(LRUCacheEvictor.class);
    if (isNondeterministic && isLRU) {
      return new NondeterministicLRUCacheEvictor(conf);
    }
    return CommonUtils.createNewClassInstance(
        conf.getClass(PropertyKey.USER_CLIENT_CACHE_EVICTOR_CLASS),
        new Class[] {AlluxioConfiguration.class}, new Object[] {conf});
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
   * @return a page to evict or null if no page available to evict
   */
  @Nullable
  PageId evict();

  /**
   * Resets the evictor.
   */
  void reset();
}
