/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.security;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.ReflectionUtils;

/**
 * A user-to-groups mapping service.
 * 
 * {@link Groups} allows for server to get the various group memberships
 * of a given user via the {@link #getGroups(String)} call, thus ensuring
 * a consistent user-to-groups mapping and protects against vagaries of
 * different mappings on servers and clients in a Tachyon cluster.
 */
public class Groups {
  private static final Logger LOG = LoggerFactory.getLogger(Groups.class);

  private final GroupMappingServiceProvider mImpl;
  private final LoadingCache<String, List<String>> mCache;
  private final long mCacheTimeout;

  public Groups() {
    this(new TachyonConf());
  }

  public Groups(TachyonConf conf) {
    mImpl =
        ReflectionUtils.newInstance(
            conf.getClass(Constants.TACHYON_SECURITY_GROUP_MAPPING,
                ShellBasedUnixGroupsMapping.class,
                GroupMappingServiceProvider.class), conf);
    mCacheTimeout =
        conf.getLong(Constants.TACHYON_SECURITY_GROUPS_CACHE_SECS,
            Constants.TACHYON_SECURITY_GROUPS_CACHE_SECS_DEFAULT) * 1000;

    mCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(mCacheTimeout, TimeUnit.MILLISECONDS)
        .expireAfterWrite(10 * mCacheTimeout, TimeUnit.MILLISECONDS)
        .ticker(new Ticker() {

          @Override
          public long read() {
            return System.nanoTime();
          }
        }).build(new GroupCacheLoader());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Group mapping impl=" + mImpl.getClass().getName()
          + "; cacheTimeout=" + mCacheTimeout);
    }
  }

  /**
   * Get the group memberships of a given user.
   * @param user User's name
   * @return the group memberships of the user
   * @throws IOException if user does not exist
   */
  public List<String> getGroups(final String user) throws IOException {
    try {
      return mCache.get(user);
    } catch (ExecutionException e) {
      throw (IOException)e.getCause();
    }
  }

  /**
   * Refresh all user-to-groups mappings.
   */
  public void cacheGroupsRefresh() {
    LOG.info("clearing userToGroupsMap cache");
    try {
      mImpl.cacheGroupsRefresh();
    } catch (IOException e) {
      LOG.warn("Error refreshing groups cache", e);
    }
    mCache.invalidateAll();
  }

  /**
   * Add groups to cache
   * 
   * @param groups list of groups to add to cache
   */
  public void cacheGroupsAdd(List<String> groups) {
    try {
      mImpl.cacheGroupsAdd(groups);
    } catch (IOException e) {
      LOG.warn("Error caching groups", e);
    }
  }

  private static Groups sGROUPS = null;

  /**
   * Get the groups being used to map user-to-groups.
   * @return the groups being used to map user-to-groups.
   */
  public static Groups getUserToGroupsMappingService() {
    return getUserToGroupsMappingService(new TachyonConf());
  }

  /**
   * Get the groups being used to map user-to-groups.
   * @param conf
   * @return the groups being used to map user-to-groups.
   */
  public static synchronized Groups getUserToGroupsMappingService( TachyonConf conf) {
    if (sGROUPS == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(" Creating new Groups object");
      }
      sGROUPS = new Groups(conf);
    }
    return sGROUPS;
  }

  /**
   * Deals with loading data into the cache.
   */
  private class GroupCacheLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String user) throws Exception {
      List<String> groups = mImpl.getGroups(user);
      if (groups.isEmpty()) {
        throw new IOException("No groups found for user " + user);
      }
      return groups;
    }
  }
}
