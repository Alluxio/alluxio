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
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;

/**
 * The Cache Filter interface which is used to determine
 * whether a particular file (URI) needs to be cached.
 */
public interface CacheFilter {

  /**
   * Create a CacheFilter.
   * @param conf the alluxio configuration
   * @return the cache filter
   */
  static CacheFilter create(AlluxioConfiguration conf) {
    return CommonUtils.createNewClassInstance(
        conf.getClass(PropertyKey.USER_CLIENT_CACHE_FILTER_CLASS),
        new Class[] {AlluxioConfiguration.class, String.class},
        new Object[] {conf, conf.getString(
            PropertyKey.USER_CLIENT_CACHE_FILTER_CONFIG_FILE)});
  }

  /**
   * Whether the specific uri needs to be cached or not.
   * @param uriStatus the uri status
   * @return whether uriStatus needs to be cached
   */
  boolean needsCache(URIStatus uriStatus);
}
