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

package alluxio.security.group;

import alluxio.Configuration;
import alluxio.Constants;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Class to map user to groups. It maintain a cache for user to groups mapping. The underlying
 * implementation depends on {@link GroupMappingService}.
 */
public class GroupMapping {
  /** The underlying implementation of GroupMappingService. */
  private GroupMappingService mEnv;
  /** Whether the cache is enabled. Set timeout to non-positive value to disable cache */
  private boolean mCacheEnabled;
  /** A loading cache for user to groups mapping, refresh periodically. */
  private LoadingCache<String, List<String>> mCache;

  /**
   * Default constructor to initialize cache.
   */
  public GroupMapping() {
    mEnv = GroupMappingService.Factory.getUserToGroupsMappingService();
    long timeoutMs = Long.parseLong(Configuration.get(
        Constants.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS));
    mCacheEnabled = timeoutMs > 0;
    if (mCacheEnabled) {
      mCache = CacheBuilder.newBuilder()
          .refreshAfterWrite(timeoutMs, TimeUnit.MILLISECONDS)
          .expireAfterWrite(10 * timeoutMs, TimeUnit.MILLISECONDS)
          .build(new GroupMappingCacheLoader());
    }
  }

  /**
   * Class to reload cache from underlying user group mapping service implementation.
   */
  private class GroupMappingCacheLoader extends CacheLoader<String, List<String>> {
    /**
     * Constructs a new {@link GroupMappingCacheLoader}.
     */
    public GroupMappingCacheLoader() {}

    @Override
    public List<String> load(String user) throws IOException {
      return mEnv.getGroups(user);
    }
  }

  /**
   * Gets a list of groups for the given user.
   *
   * @param user user name
   * @return the list of groups that the user belongs to
   * @throws IOException if failed to get groups
   */
  public List<String> getGroups(String user) throws IOException {
    if (!mCacheEnabled) {
      return mEnv.getGroups(user);
    }
    try {
      return mCache.get(user);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }
}
