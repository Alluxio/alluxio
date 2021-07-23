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

package alluxio.client.file.cache.filter;

import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;

/**
 * Default Cache Filter cache everything.
 */
public class DefaultCacheFilter implements CacheFilter {
  /**
   * The default constructor.
   * @param conf the Alluxio Configuration
   * @param cacheConfigFile the cache config giles
   */
  public DefaultCacheFilter(AlluxioConfiguration conf, String cacheConfigFile) {
  }

  /**
   * The implementation of needsCache.
   * @param uriStatus the uri
   * @return true for everything
   */
  public boolean needsCache(URIStatus uriStatus) {
    return true;
  }
}
