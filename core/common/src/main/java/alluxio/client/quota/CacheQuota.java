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

/**
 * Data structure that returns cache size in bytes associated with a cache scope.
 */
public interface CacheQuota {
  /**
   * A predefined CacheQuota instance that sets NO limit.
   */
  CacheQuota UNLIMITED = (CacheScope cacheScope) -> Long.MAX_VALUE;

  /**
   * @param cacheScope the scope to query
   * @return size of quota of this scope in bytes
   */
  long getQuota(CacheScope cacheScope);
}
