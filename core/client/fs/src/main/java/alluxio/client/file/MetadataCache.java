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

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Cache for metadata of paths.
 */
@ThreadSafe
public final class MetadataCache {
  private final Cache<String, URIStatus> mCache;
  private final Cache<String, List<URIStatus>> mDirCache;

  /**
   * @param maxSize the max size of the cache
   * @param expirationTimeMs the expiration time (in milliseconds) of the cached item
   */
  public MetadataCache(int maxSize, long expirationTimeMs) {
    mCache = CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterWrite(expirationTimeMs, TimeUnit.MILLISECONDS)
        .build();
    mDirCache = CacheBuilder.newBuilder()
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
    return mCache.getIfPresent(path.getPath());
  }

  /**
   * @param path the Alluxio path
   * @param status the status to be cached
   */
  public void put(AlluxioURI path, URIStatus status) {
    mCache.put(path.getPath(), status);
  }

  /**
   * @param path the Alluxio path
   * @param status the status to be cached
   */
  public void put(String path, URIStatus status) {
    mCache.put(path, status);
  }

  /**
   * Caches list status results of a directory.
   *
   * @param dir the directory
   * @param statuses the list status results
   */
  public void put(AlluxioURI dir, List<URIStatus> statuses) {
    mDirCache.put(dir.getPath(), statuses);
    for (URIStatus status : statuses) {
      mCache.put(status.getPath(), status);
    }
  }

  /**
   * @param dir the directory
   * @return the cached list status results or null
   */
  @Nullable
  public List<URIStatus> listStatus(AlluxioURI dir) {
    return mDirCache.getIfPresent(dir.getPath());
  }

  /**
   * @return the cache size
   */
  @VisibleForTesting
  public long size() {
    return mCache.size();
  }
}
