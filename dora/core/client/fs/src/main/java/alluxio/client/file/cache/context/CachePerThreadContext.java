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

package alluxio.client.file.cache.context;

/**
 * A per-thread cache context for local cache.
 */
public class CachePerThreadContext {
  private static ThreadLocal<CachePerThreadContext> sContext =
      ThreadLocal.withInitial(() -> new CachePerThreadContext());

  private boolean mCacheEnabled = true;

  /**
   * @return per-thread cache context
   */
  public static CachePerThreadContext get() {
    return sContext.get();
  }

  /**
   * @param isCacheEnabled
   */
  public void setCacheEnabled(boolean isCacheEnabled) {
    mCacheEnabled = isCacheEnabled;
  }

  /**
   * @return if cache is enabled
   */
  public boolean getCacheEnabled() {
    return mCacheEnabled;
  }
}
