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

package alluxio.client.quota;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

/**
 * Data structure that stores and returns cache size in number of bytes associated with a
 * cache scope.
 */
public class CacheQuota {
  /**
   * A predefined CacheQuota instance that sets NO limit.
   */
  public static final CacheQuota UNLIMITED = new CacheQuota() {
    public long getQuota(CacheScope cacheScope) {
      return Long.MAX_VALUE;
    }
  };

  private final Map<CacheScope.Level, Long> mQuota;

  /**
   * @param quota a map from scope level to size in bytes
   */
  public CacheQuota(Map<CacheScope.Level, Long> quota) {
    mQuota = quota;
  }

  /**
   * Empty cache quota.
   */
  public CacheQuota() {
    this(ImmutableMap.of());
  }

  /**
   * @param cacheScope the scope to query
   * @return size of quota of this scope in bytes
   */
  public long getQuota(CacheScope cacheScope) {
    return mQuota.getOrDefault(cacheScope.level(), Long.MAX_VALUE);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CacheQuota that = (CacheQuota) o;
    return Objects.equals(mQuota, that.mQuota);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mQuota);
  }
}
