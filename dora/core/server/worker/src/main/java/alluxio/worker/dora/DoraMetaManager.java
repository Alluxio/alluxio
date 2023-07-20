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

package alluxio.worker.dora;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.FileId;
import alluxio.grpc.FileInfo;
import alluxio.proto.meta.DoraMeta;
import alluxio.proto.meta.DoraMeta.FileStatus;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.GetStatusOptions;
import alluxio.underfs.options.ListOptions;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

/**
 * The Dora metadata manager that orchestrates the metadata operations.
 *
 * TODO(elega) Invalidating page cache synchronously causes performance issue and currently it
 *  also lacks concurrency control. Address this problem in the future.
 */
public class DoraMetaManager implements Closeable {
  private final DoraMetaStore mMetaStore;
  private final CacheManager mCacheManager;
  private final PagedDoraWorker mDoraWorker;
  private final UnderFileSystem mUfs;

  private final long mListingCacheCapacity
      = Configuration.getInt(PropertyKey.DORA_UFS_LIST_STATUS_CACHE_NR_FILES);
  private final boolean mGetRealContentHash
      = Configuration.getBoolean(PropertyKey.USER_FILE_METADATA_LOAD_REAL_CONTENT_HASH);
  private final Cache<String, ListStatusResult> mListStatusCache = mListingCacheCapacity == 0
      ? null
      : Caffeine.newBuilder()
      .maximumWeight(mListingCacheCapacity)
      .weigher((String k, ListStatusResult v) ->
          v.mUfsStatuses == null ? 0 : v.mUfsStatuses.length)
      .expireAfterWrite(Configuration.getDuration(PropertyKey.DORA_UFS_LIST_STATUS_CACHE_TTL))
      .build();

  /**
   * Creates a dora meta manager.
   * @param doraWorker the dora worker instance
   * @param cacheManger the cache manager to manage the page cache
   * @param ufs the associated ufs
   */
  public DoraMetaManager(
      PagedDoraWorker doraWorker, CacheManager cacheManger,
      UnderFileSystem ufs) {
    String dbDir = Configuration.getString(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR);
    Duration duration = Configuration.getDuration(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_TTL);
    long ttl = (duration.isNegative() || duration.isZero()) ? -1 : duration.getSeconds();
    mMetaStore = new RocksDBDoraMetaStore(dbDir, ttl);
    mCacheManager = cacheManger;
    mDoraWorker = doraWorker;
    mUfs = ufs;
  }

  /**
   * Gets file meta from UFS.
   * @param path the full ufs path
   * @return the file status, or empty optional if not found
   */
  public Optional<FileStatus> getFromUfs(String path) throws IOException {
    try {
      UfsStatus status = mUfs.getStatus(path,
          GetStatusOptions.defaults().setIncludeRealContentHash(mGetRealContentHash));
      DoraMeta.FileStatus fs = mDoraWorker.buildFileStatusFromUfsStatus(status, path);
      return Optional.ofNullable(fs);
    } catch (FileNotFoundException e) {
      return Optional.empty();
    }
  }

  /**
   * Gets file meta from UFS and loads it into metastore if exists.
   * If the file does not exist in the UFS, clean up metadata and data.
   *
   * @param path the full ufs path
   * @return the file status, or empty optional if not found
   */
  public Optional<FileStatus> loadFromUfs(String path) throws IOException {
    Optional<FileStatus> fileStatus = getFromUfs(path);
    if (!fileStatus.isPresent()) {
      removeFromMetaStore(path);
    } else {
      put(path, fileStatus.get());
    }
    // TODO(elega) invalidate/update listing cache based on the load result
    return fileStatus;
  }

  /**
   * Gets file meta from the metastore.
   * @param path the full ufs path
   * @return the file status, or empty optional if not found
   */
  public Optional<FileStatus> getFromMetaStore(String path) {
    return mMetaStore.getDoraMeta(path);
  }

  /**
   * Puts meta of a file into the metastore, and invalidates the file data cache.
   * @param path the full ufs path
   * @param status the file meta
   */
  public void put(String path, FileStatus status) {
    Optional<FileStatus> existingStatus = mMetaStore.getDoraMeta(path);
    if (!existingStatus.isPresent()
        || existingStatus.get().getFileInfo().getFolder()
        || existingStatus.get().getFileInfo().getLength() == 0) {
      mMetaStore.putDoraMeta(path, status);
      return;
    }
    if (shouldInvalidatePageCache(existingStatus.get().getFileInfo(), status.getFileInfo())) {
      invalidateCachedFile(path);
    }
    mMetaStore.putDoraMeta(path, status);
  }

  /**
   * Removes meta of a file from the meta store.
   * @param path the full ufs path
   * @return the removed file meta, if exists
   */
  public Optional<FileStatus> removeFromMetaStore(String path) {
    invalidateListingCache(getPathParent(path));
    Optional<FileStatus> status = mMetaStore.getDoraMeta(path);
    if (status.isPresent()) {
      mMetaStore.removeDoraMeta(path);
    }
    invalidateCachedFile(path);
    return status;
  }

  /**
   * Invalidates the listing cache of a given path.
   * @param path the full ufs path
   */
  public void invalidateListingCache(String path) {
    if (mListStatusCache != null) {
      mListStatusCache.invalidate(path);
    }
  }

  /**
   * Invalidates the listing cache of its parent of a given path.
   * If root is specified, the listing cache of root itself will be invalidated.
   * @param path the full ufs path
   */
  public void invalidateListingCacheOfParent(String path) {
    if (mListStatusCache != null) {
      mListStatusCache.invalidate(getPathParent(path));
    }
  }

  /**
   * Get the cached listing result from the listing cache.
   * @param path the full ufs path to list
   * @param isRecursive if the list is recursive
   * @return an Optional of a listStatusResult object. If the object exists but the ufs status
   * is empty, it means that the directory does not exist.
   */
  public Optional<ListStatusResult> listCached(String path, boolean isRecursive) {
    if (mListStatusCache == null) {
      return Optional.empty();
    }
    // We don't cache recursive listing result as usually the number of files are too
    // large to cache.
    if (isRecursive) {
      return Optional.empty();
    }
    ListStatusResult result = mListStatusCache.getIfPresent(path);
    return Optional.ofNullable(result);
  }

  /**
   * Lists a directory from UFS and cache it into the listing cache if it exists.
   * @param path the ufs path
   * @param isRecursive if the listing is recursive
   * @return an empty option if the directory does not exist or
   * the path does not denote a directory,
   * otherwise an option contains an ufs status array.
   * @throws IOException if the UFS call failed
   */
  public Optional<UfsStatus[]> listFromUfsThenCache(String path, boolean isRecursive)
      throws IOException {
    // Recursive listing results are not cached.
    if (mListStatusCache == null || isRecursive) {
      return listFromUfs(path, isRecursive);
    }
    try {
      ListStatusResult cached = mListStatusCache.get(path, (k) -> {
        try {
          Optional<UfsStatus[]> listResults = listFromUfs(path, false);
          return listResults.map(
                  ufsStatuses -> new ListStatusResult(
                      System.nanoTime(), ufsStatuses,
                      ufsStatuses.length == 1 && ufsStatuses[0].isFile()
                  ))
              // This cache also serves as absent cache, so we persist a NULL (not empty) result,
              // if the path not found or is not a directory.
              .orElseGet(() -> new ListStatusResult(System.nanoTime(), null, false));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      if (cached == null) {
        return Optional.empty();
      } else {
        return Optional.ofNullable(cached.mUfsStatuses);
      }
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Lists a directory from UFS.
   * @param path the ufs path
   * @param isRecursive if the listing is recursive
   * @return an empty option if the directory does not exist or
   * the path does not denote a directory,
   * otherwise an option contains an ufs status array.
   * @throws IOException if the UFS call failed
   */
  public Optional<UfsStatus[]> listFromUfs(String path, boolean isRecursive)
      throws IOException {
    ListOptions ufsListOptions = ListOptions.defaults().setRecursive(isRecursive);
    try {
      UfsStatus[] listResults = mUfs.listStatus(path, ufsListOptions);
      if (listResults != null) {
        return Optional.of(listResults);
      }
    } catch (IOException e) {
      if (!(e instanceof FileNotFoundException)) {
        throw e;
      }
    }
    // TODO(yimin) put the ufs status into the metastore
    // If list does not give a result,
    // the request path might either be a regular file/object or not exist.
    // Try getStatus() instead.
    try {
      UfsStatus status = mUfs.getStatus(path);
      if (status == null) {
        return Optional.empty();
      }
      // Success. Create an array with only one element.
      status.setName(""); // listStatus() expects relative name to the @path.
      return Optional.of(new UfsStatus[] {status});
    } catch (FileNotFoundException e) {
      return Optional.empty();
    }
  }

  /**
   * Decides if the page cache should be invalidated if the file metadata is updated.
   * Similar to {@link alluxio.underfs.Fingerprint#matchContent(Fingerprint)},
   * if the update metadata matches any of the following, we consider the page cache
   * should be invalidated:
   * 1. the file type changed (from file to directory or directory to file)
   * 2. the ufs type changed (e.g. from s3 to hdfs)
   * 3. the file content does not match or is null
   * @param origin the origin file info from metastore
   * @param updated the updated file into to add to the metastore
   * @return true if the page cache (if any) should be invalidated, otherwise false
   */
  private boolean shouldInvalidatePageCache(FileInfo origin, FileInfo updated) {
    if (!mUfs.getUnderFSType().equals(origin.getUfsType())) {
      return true;
    }
    if (origin.getFolder() != updated.getFolder()) {
      return true;
    }
    // Keep the page cache in the most conservative way.
    // If content hash not set, page cache will be cleared.
    return Strings.isNullOrEmpty(origin.getContentHash())
        || Strings.isNullOrEmpty(updated.getContentHash())
        || !origin.getContentHash().equals(updated.getContentHash());
  }

  private void invalidateCachedFile(String path) {
    FileId fileId = FileId.of(AlluxioURI.hash(path));
    mCacheManager.deleteFile(fileId.toString());
  }

  private String getPathParent(String path) {
    AlluxioURI fullPathUri = new AlluxioURI(path);
    AlluxioURI parentDir;
    if (fullPathUri.isRoot()) {
      parentDir = fullPathUri;
    } else {
      parentDir = fullPathUri.getParent();
      Preconditions.checkNotNull(parentDir);
    }
    return parentDir.toString();
  }

  @Override
  public void close() throws IOException {
    mMetaStore.close();
  }
}
