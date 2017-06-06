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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for recording information about paths that are not present in UFS.
 */
public interface UfsAbsentPathCache {
  /**
   * Processes the given path for the cache. This will sequentially walk down the path to find
   * components which do and do not exist in the UFS, and updates the cache accordingly.
   *
   * @param path the path to process for the cache
   */
  void process(AlluxioURI path);

  /**
   * Processes the given path that already exists. This will sequentially walk down the path and
   * update the cache accordingly.
   *
   * @param path the path to process for the cache
   */
  void processExisting(AlluxioURI path);

  /**
   * Returns true if the given path is absent, according to this cache. A path is absent if one of
   * its ancestors is absent.
   *
   * @param path the path to check
   * @return true if the path is absent according to the cache
   */
  boolean isAbsent(AlluxioURI path);

  /**
   * Factory class for {@link UfsAbsentPathCache}.
   */
  final class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(UfsAbsentPathCache.Factory.class);

    private Factory() {} // prevent instantiation

    public static UfsAbsentPathCache create(MountTable mountTable) {
      int numThreads = Configuration.getInt(PropertyKey.MASTER_UFS_PATH_CACHE_THREADS);
      if (numThreads <= 0) {
        LOG.info("UfsAbsentPathCache is disabled. {}: {}",
            PropertyKey.MASTER_UFS_PATH_CACHE_THREADS, numThreads);
        return new NoopUfsAbsentPathCache();
      }
      return new AsyncUfsAbsentPathCache(mountTable, numThreads);
    }
  }
}
