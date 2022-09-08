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

package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Cache for metadata of paths.
 */
@ThreadSafe
public final class FuseMetadataCache {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataCache.class);
  private static final int MAX_ASYNC_RELEASE_WAITTIME_MS = 5000;
  private final MetadataCache mMetadataCache;
  private final FileSystem mFileSystem;
  
  /**
   * @param fileSystem the filesystem
   */
  public FuseMetadataCache(FileSystem fileSystem, int maxSize, long expirationTimeMs) {
    mFileSystem = fileSystem;
    mMetadataCache = new MetadataCache(maxSize, expirationTimeMs);
  }
  
  /**
   * Gets the path status.
   *
   * @param uri the Alluxio uri to get status of
   * @return the file status
   */
  public Optional<FuseURIStatus> getPathStatus(AlluxioURI uri) {
    Optional<FuseURIStatus> uriStatus = mMetadataCache.get(uri);
    if (uriStatus.isPresent()) {
      return uriStatus;
    }
    try {
      FuseURIStatus status = new FuseURIStatus(mFileSystem.getStatus(uri));
      mMetadataCache.put(uri, status);
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
   */
  public Optional<FuseURIStatus> waitForFileCompleted(AlluxioURI uri) {
    Optional<FuseURIStatus> uriStatus = mMetadataCache.get(uri);
    if (uriStatus.isPresent() && uriStatus.get().isCompleted()) {
      return uriStatus;
    }
    try {
      URIStatus status = CommonUtils.waitForResult("file completed", () -> {
            try {
              return mFileSystem.getStatus(uri);
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Unexpected error while getting backup status: %s", e));
            }
          }, URIStatus::isCompleted,
          WaitForOptions.defaults().setTimeoutMs(MAX_ASYNC_RELEASE_WAITTIME_MS));
      FuseURIStatus fuseStatus = new FuseURIStatus(status);
      mMetadataCache.put(uri, fuseStatus);
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
      mMetadataCache.invalidate(uri);
      mFileSystem.delete(uri);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(String.format("Failed to delete path %s", uri), e);
    }
  }
  
  public void invalidate(AlluxioURI uri) {
    mMetadataCache.invalidate(uri);
  }

  public static class FuseURIStatus {
    private String mName = "";
    private long mLength;
    private final boolean mCompleted;
    private boolean mFolder;
    private long mLastModificationTimeMs;
    private long mLastAccessTimeMs;
    private String mOwner = "";
    private String mGroup = "";
    private int mMode;

    public FuseURIStatus(URIStatus uriStatus) {
      mName = uriStatus.getName();
      mLength = uriStatus.getLength();
      mCompleted = uriStatus.isCompleted();
      mFolder = uriStatus.isFolder();
      mLastModificationTimeMs = uriStatus.getLastModificationTimeMs();
      mLastAccessTimeMs = uriStatus.getLastAccessTimeMs();
      mOwner = uriStatus.getOwner();
      mGroup = uriStatus.getGroup();
      mMode = uriStatus.getMode();
    }

    public FuseURIStatus(String name, long length, boolean completed, long lastModificationTimeMs, 
        long lastAccessTime, String owner, String group, int mode) {
      mName = name;
      mLength = length;
      mCompleted = completed;
      mFolder = isFolder();
      mLastModificationTimeMs = lastModificationTimeMs;
      mLastAccessTimeMs = lastAccessTime;
      mOwner = owner;
      mGroup = group;
      mMode = mode;
    }
    
    public FuseURIStatus(boolean completed) {
      mCompleted = completed;
    }

    /**
     * @return the file name
     */
    public String getName() {
      return mName;
    }

    /**
     * @return the file length
     */
    public long getLength() {
      return mLength;
    }


    /**
     * @return the file last modification time (in milliseconds)
     */
    public long getLastModificationTimeMs() {
      return mLastModificationTimeMs;
    }

    /**
     * @return the file last access time (in milliseconds)
     */
    public long getLastAccessTimeMs() {
      return mLastAccessTimeMs;
    }


    /**
     * @return the file owner
     */
    public String getOwner() {
      return mOwner;
    }

    /**
     * @return the file owner group
     */
    public String getGroup() {
      return mGroup;
    }

    /**
     * @return the file mode bits
     */
    public int getMode() {
      return mMode;
    }

    /**
     * @return whether the file is completed
     */
    public boolean isCompleted() {
      return mCompleted;
    }

    /**
     * @return whether the file is a folder
     */
    public boolean isFolder() {
      return mFolder;
    }
  }
  
  class MetadataCache {
    
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
      invalidate(path);
    }

    /**
     * Invalidates all the cache.
     */
    public void invalidateAll() {
      mCache.invalidateAll();
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
