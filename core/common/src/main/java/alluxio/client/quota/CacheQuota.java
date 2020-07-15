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

/**
 * Data structure to store and return number of bytes associated with a scope.
 */
public class CacheQuota {
  public static final CacheQuota UNLIMITED =
      new CacheQuota(ImmutableMap.of(Scope.GLOBAL, Long.MAX_VALUE));

  private final Map<Scope, Long> mQuota;

  /**
   * @param quota a map from scope to size in bytes
   */
  public CacheQuota(Map<Scope, Long> quota) {
    mQuota = quota;
  }

  /**
   * @param scope the scope to query
   * @return size of quota of this scope in bytes
   */
  public long getQuota(Scope scope) {
    return mQuota.get(scope);
  }
}
