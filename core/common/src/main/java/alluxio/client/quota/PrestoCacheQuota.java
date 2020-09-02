/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.client.quota;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Data structure that stores and returns cache size in number of bytes associated with a
 * cache scope.
 */
public class PrestoCacheQuota implements CacheQuota {
  private final Map<PrestoCacheScope.Level, Long> mQuota;

  /**
   * @param quota a map from scope level to size in bytes
   */
  public PrestoCacheQuota(Map<PrestoCacheScope.Level, Long> quota) {
    mQuota = quota;
  }

  /**
   * @param level scope level
   * @param bytes bytes for this scope level
   */
  public PrestoCacheQuota(PrestoCacheScope.Level level, long bytes) {
    mQuota = ImmutableMap.of(level, bytes);
  }

  /**
   * @param cacheScope the scope to query
   * @return size of quota of this scope in bytes
   */
  @Override
  public long getQuota(CacheScope cacheScope) {
    Preconditions.checkArgument(cacheScope instanceof PrestoCacheScope);
    PrestoCacheScope ps = (PrestoCacheScope) cacheScope;
    return mQuota.getOrDefault(ps.level(), Long.MAX_VALUE);
  }
}
