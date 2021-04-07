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

import javax.annotation.concurrent.ThreadSafe;

/**
 * A client-side cache eviction policy that evicts nothing for debugging purposes.
 */
@ThreadSafe
public class UnevictableCacheEvictor implements CacheEvictor {
  /**
   * Required constructor.
   *
   * @param conf Alluxio configuration
   */
  public UnevictableCacheEvictor(AlluxioConfiguration conf) {}

  @Override
  public void updateOnGet(PageId pageId) {}

  @Override
  public void updateOnPut(PageId pageId) {}

  @Override
  public void updateOnDelete(PageId pageId) {}

  @Override
  public PageId evict() {
    return null;
  }

  @Override
  public void reset() {}
}
