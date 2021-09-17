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

package alluxio.table.under.hive.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements a cache for Client connection pools for Hive Clients.
 */
public class HiveClientPoolCache {
  private final Map<String, AbstractHiveClientPool> mClientPools;

  /**
   * Constructor for the Client Pool cache.
   */
  public HiveClientPoolCache() {
    mClientPools = new ConcurrentHashMap<>();
  }

  /**
   * Get a hive client pool from the cache.
   *
   * @param connectionURI connection which serves as key
   * @return hive client pool
   */
  public AbstractHiveClientPool getPool(String connectionURI) {
    return mClientPools.compute(connectionURI, (uri, pool) -> {
      if (pool != null) {
        return pool;
      } else {
        return new DefaultHiveClientPool(connectionURI);
      }
    });
  }
}
