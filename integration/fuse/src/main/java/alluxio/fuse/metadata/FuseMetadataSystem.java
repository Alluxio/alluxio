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

package alluxio.fuse.metadata;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Fuse metadata system for metadata related operations.
 */
@ThreadSafe
public final class FuseMetadataSystem {
  private static final int MAX_ASYNC_RELEASE_WAITTIME_MS = 5000;
  private final FileSystem mFileSystem;
  private final Optional<MetadataCache> mMetadataCache;

  /**
   * @param fileSystem the filesystem
   * @param conf the Alluxio configuration
   */
  public FuseMetadataSystem(FileSystem fileSystem, AlluxioConfiguration conf) {
    mFileSystem = fileSystem;
    mMetadataCache = conf.getBoolean(PropertyKey.FUSE_METADATA_CACHE_ENABLED)
        ? Optional.of(new MetadataCache(conf.getInt(PropertyKey.FUSE_METADATA_CACHE_MAX_SIZE),
        conf.getMs(PropertyKey.FUSE_METADATA_CACHE_EXPIRATION_TIME)))
        : Optional.empty();
  }

  /**
   * Gets the path status.
   *
   * @param uri the Alluxio uri to get status of
   * @return the file status
   */
  public Optional<FuseURIStatus> getPathStatus(AlluxioURI uri) {
    if (mMetadataCache.isPresent()) {
      Optional<FuseURIStatus> uriStatus = mMetadataCache.get().get(uri);
      if (uriStatus.isPresent()) {
        return uriStatus;
      }
    }
    try {
      FuseURIStatus status = FuseURIStatus.create(mFileSystem.getStatus(uri));
      mMetadataCache.ifPresent(metadataCache -> metadataCache.put(uri, status));
      return Optional.of(status);
    } catch (InvalidPathException | FileNotFoundException | FileDoesNotExistException e) {
      return Optional.empty();
    } catch (IOException | AlluxioException ex) {
      throw new RuntimeException(String.format("Failed to get path status of %s", uri), ex);
    }
  }

  /**
   * Waits for the file to complete. This method is mainly added to make sure
   * the async release() when writing a file finished before getting status of
   * the file or opening the file for read().
   *
   * @param uri the file path to check
   * @return the {@link FuseURIStatus}
   */
  public Optional<FuseURIStatus> waitForFileCompleted(AlluxioURI uri) {
    if (mMetadataCache.isPresent()) {
      Optional<FuseURIStatus> uriStatus = mMetadataCache.get().get(uri);
      if (uriStatus.isPresent() && uriStatus.get().isCompleted()) {
        return uriStatus;
      }
    }
    try {
      URIStatus status = CommonUtils.waitForResult("file completed", () -> {
        try {
          return mFileSystem.getStatus(uri);
        } catch (Exception e) {
          throw new RuntimeException(String.format(
              "Unexpected error while waiting for file %s to complete", uri), e);
        }
      }, URIStatus::isCompleted,
          WaitForOptions.defaults().setTimeoutMs(MAX_ASYNC_RELEASE_WAITTIME_MS));
      FuseURIStatus fuseStatus = FuseURIStatus.create(status);
      mMetadataCache.ifPresent(metadataCache -> metadataCache.put(uri, fuseStatus));
      return Optional.of(fuseStatus);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return Optional.empty();
    } catch (TimeoutException te) {
      return Optional.empty();
    }
  }

  /**
   * Deletes a file or a directory in alluxio namespace.
   *
   * @param uri the alluxio uri
   */
  public void deletePath(AlluxioURI uri) {
    try {
      invalidate(uri);
      mFileSystem.delete(uri);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(String.format("Failed to delete path %s", uri), e);
    }
  }

  /**
   * Invalidate an Alluxio URi cache.
   *
   * @param uri the uri to invalidate
   */
  public void invalidate(AlluxioURI uri) {
    mMetadataCache.ifPresent(metadataCache -> metadataCache.invalidate(uri));
  }

  static class MetadataCache {

    private final Cache<AlluxioURI, FuseURIStatus> mCache;

    /**
     * @param maxSize          the max size of the cache
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
    public Optional<FuseURIStatus> get(AlluxioURI path) {
      FuseURIStatus status = mCache.getIfPresent(path);
      return status == null ? Optional.empty() : Optional.of(status);
    }

    /**
     * @param path   the Alluxio path
     * @param status the status to be cached
     */
    public void put(AlluxioURI path, FuseURIStatus status) {
      mCache.put(path, status);
    }

    /**
     * Invalidates the cache of path.
     *
     * @param path the path
     */
    public void invalidate(AlluxioURI path) {
      mCache.invalidate(path);
    }

    /**
     * @return the cache size
     */
    @VisibleForTesting
    public long size() {
      return mCache.size();
    }
  }
}
