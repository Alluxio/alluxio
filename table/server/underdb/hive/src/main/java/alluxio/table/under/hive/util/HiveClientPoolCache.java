package alluxio.table.under.hive.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements a cache for Client connection pools for Hive Clients.
 */
public class HiveClientPoolCache {
  private final Map<String, HiveClientPool> mClientPools;

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
  public HiveClientPool getPool(String connectionURI) {
    return mClientPools.compute(connectionURI, (uri, pool) -> {
      if (pool != null) {
        return pool;
      } else {
        return new HiveClientPool(connectionURI);
      }
    });
  }
}
