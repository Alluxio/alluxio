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

package alluxio.client.file;

import alluxio.AlluxioURI;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Cache for metadata of paths.
 */
@ThreadSafe
public final class MetadataCache {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataCache.class);

  private class CachedItem {
    private URIStatus mStatus = null;
    private List<URIStatus> mDirStatuses = null;

    /**
     * @return the metadata of the path
     */
    @Nullable
    public URIStatus getStatus() {
      return mStatus;
    }

    /**
     * @return the metadata list of paths directly under a directory
     */
    @Nullable
    public List<URIStatus> getDirStatuses() {
      return mDirStatuses;
    }

    /**
     *  Puts the status into cache.
     *
     *  @param status the metadata of the path
     */
    public void setStatus(URIStatus status) {
      mStatus = status;
    }

    /**
     *  Puts the directory status into cache.
     *
     *  @param statuses the metadata list
     */
    public void setDirStatuses(List<URIStatus> statuses) {
      mDirStatuses = statuses;
    }
  }

  private final Cache<String, CachedItem> mCache;

  /**
   * @param maxSize the max size of the cache
   * @param expirationTimeMs the expiration time (in milliseconds) of the cached item
   */
  public MetadataCache(int maxSize, long expirationTimeMs) {
    mCache = CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterWrite(expirationTimeMs, TimeUnit.MILLISECONDS)
        .build();
  }

  /**
   * @param path the Alluxio path
   * @return the cached status or null
   */
  @Nullable
  public URIStatus get(AlluxioURI path) {
    CachedItem item = mCache.getIfPresent(path.getPath());
    if (item != null && item.getStatus() != null) {
      return item.getStatus();
    }
    return null;
  }

  /**
   * @param path the Alluxio path
   * @param status the status to be cached
   */
  public void put(AlluxioURI path, URIStatus status) {
    put(path.getPath(), status);
  }

  /**
   * @param path the Alluxio path
   * @param status the status to be cached
   */
  public void put(String path, URIStatus status) {
    try {
      CachedItem item = mCache.get(path, () -> new CachedItem());
      item.setStatus(status);
    } catch (ExecutionException e) {
      LOG.warn("Failed to cache meta data for path {}", path);
    }
  }

  /**
   * Caches list status results of a directory.
   *
   * @param dir the directory
   * @param statuses the list status results
   */
  public void put(AlluxioURI dir, List<URIStatus> statuses) {
    try {
      CachedItem item = mCache.get(dir.getPath(), () -> new CachedItem());
      item.setDirStatuses(statuses);
      for (URIStatus status : statuses) {
        put(status.getPath(), status);
      }
    } catch (ExecutionException e) {
      LOG.warn("Failed to cache metadata for dir {}", dir.getPath());
    }
  }

  /**
   * @param dir the directory
   * @return the cached list status results or null
   */
  @Nullable
  public List<URIStatus> listStatus(AlluxioURI dir) {
    CachedItem item = mCache.getIfPresent(dir.getPath());
    if (item != null && item.getDirStatuses() != null) {
      return item.getDirStatuses();
    }
    return null;
  }

  /**
   * Invalidates the cache of path.
   *
   * @param path the path
   */
  public void invalidate(AlluxioURI path) {
    mCache.invalidate(path.getPath());
  }

  /**
   * @return the cache size
   */
  @VisibleForTesting
  public long size() {
    return mCache.size();
  }
}
