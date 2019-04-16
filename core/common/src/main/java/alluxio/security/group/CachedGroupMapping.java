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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Class to map user to groups. It maintains a cache for user to groups mapping. The underlying
 * implementation depends on {@link GroupMappingService}.
 */
public class CachedGroupMapping implements GroupMappingService {
  /** The underlying implementation of GroupMappingService. */
  private final GroupMappingService mService;
  /** Whether the cache is enabled. Set timeout to non-positive value to disable cache. */
  private final boolean mCacheEnabled;
  /** A loading cache for user to groups mapping, refresh periodically. */
  private LoadingCache<String, List<String>> mCache;
  /** Max size of the cache. */
  private static final long MAXSIZE = 10000;

  /** Create an executor service that provide ListenableFuture instances. */
  private final ThreadFactory mThreadFactory = new ThreadFactoryBuilder()
      .setNameFormat("UserGroupMappingCachePool-%d").setDaemon(true).build();
  private final ExecutorService mParentExecutor = Executors.newSingleThreadExecutor(mThreadFactory);
  private final ListeningExecutorService mExecutorService =
      MoreExecutors.listeningDecorator(mParentExecutor);

  /**
   * Constructor with specified {@link GroupMappingService}. Initializes the cache if enabled.
   *
   * @param service group mapping service
   * @param groupMappingCacheTimeoutMs The timeout in millseconds before we should reload the cache
   */
  public CachedGroupMapping(GroupMappingService service, long groupMappingCacheTimeoutMs) {
    mService = service;
    mCacheEnabled = groupMappingCacheTimeoutMs > 0;
    if (mCacheEnabled) {
      mCache = CacheBuilder.newBuilder()
          // the maximum number of entries the cache may contain.
          .maximumSize(MAXSIZE)
          // active entries are eligible for automatic refresh once the specified time duration has
          // elapsed after the entry was last modified.
          .refreshAfterWrite(groupMappingCacheTimeoutMs, TimeUnit.MILLISECONDS)
          // each entry should be automatically removed from the cache once the specified time
          // duration has elapsed after the entry was last modified.
          .expireAfterWrite(10 * groupMappingCacheTimeoutMs, TimeUnit.MILLISECONDS)
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
      return mService.getGroups(user);
    }

    @Override
    public ListenableFuture<List<String>> reload(final String user, List<String> oldValue)
        throws IOException {
      // Load values asynchronously.
      ListenableFuture<List<String>> listenableFuture = mExecutorService.submit(
          () -> load(user)
      );
      return listenableFuture;
    }
  }

  /**
   * Gets a list of groups for the given user.
   *
   * @param user user name
   * @return the list of groups that the user belongs to
   */
  public List<String> getGroups(String user) throws IOException {
    if (!mCacheEnabled) {
      return mService.getGroups(user);
    }
    try {
      return mCache.get(user);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }
}
