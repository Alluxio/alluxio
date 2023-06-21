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
import alluxio.client.file.cache.PageId;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.FileId;
import alluxio.proto.meta.DoraMeta;
import alluxio.proto.meta.DoraMeta.FileStatus;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;

import com.google.common.annotations.VisibleForTesting;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

/**
 * The Dora metadata manager that orchestrates the metadata operations.
 *
 * TODO(elega) Invalidating page cache synchronously causes performance issue and currently it
 *  also lacks concurrency control. Address this problem in the future.
 */
public class DoraMetaManager {
  private final DoraMetaStore mMetastore;
  private final CacheManager mCacheManager;
  private final PagedDoraWorker mDoraWorker;
  private final UnderFileSystem mUfs;
  private boolean mPopulateMetadataFingerprint =
      Configuration.getBoolean(PropertyKey.DORA_WORKER_POPULATE_METADATA_FINGERPRINT);

  /**
   * Creates a dora meta manager.
   * @param doraWorker the dora worker instance
   * @param metaStore the dora meta store
   * @param cacheManger the cache manager to manage the page cache
   * @param ufs the associated ufs
   */
  public DoraMetaManager(
      PagedDoraWorker doraWorker, DoraMetaStore metaStore, CacheManager cacheManger,
      UnderFileSystem ufs) {
    mMetastore = metaStore;
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
      UfsStatus status = mUfs.getStatus(path);
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
    if (fileStatus.isEmpty()) {
      removeFromMetaStore(path);
    } else {
      put(path, fileStatus.get());
    }
    return fileStatus;
  }

  /**
   * Gets file meta from the metastore.
   * @param path the full ufs path
   * @return the file status, or empty optional if not found
   */
  public Optional<FileStatus> getFromMetaStore(String path) {
    return mMetastore.getDoraMeta(path);
  }

  /**
   * Puts meta of a file into the metastore, and invalidates the file data cache.
   * @param path the full ufs path
   * @param meta the file meta
   */
  public void put(String path, FileStatus meta) {
    Optional<FileStatus> status = mMetastore.getDoraMeta(path);
    if (status.isEmpty()
        || status.get().getFileInfo().getFolder()
        || status.get().getFileInfo().getLength() == 0) {
      mMetastore.putDoraMeta(path, meta);
      return;
    }
    if (mPopulateMetadataFingerprint) {
      Fingerprint originalFingerprint =
          Fingerprint.parse(status.get().getFileInfo().getUfsFingerprint());
      if (originalFingerprint == null) {
        originalFingerprint = Fingerprint.INVALID_FINGERPRINT;
      }
      Fingerprint newFingerprint =
          Fingerprint.parse(meta.getFileInfo().getUfsFingerprint());
      if (newFingerprint == null) {
        newFingerprint = Fingerprint.INVALID_FINGERPRINT;
      }
      boolean contentMatched = originalFingerprint.isValid()
          && newFingerprint.isValid()
          && originalFingerprint.matchContent(newFingerprint);
      if (!contentMatched) {
        invalidateCachedFile(path, status.get().getFileInfo().getLength());
      }
    } else {
      invalidateCachedFile(path, status.get().getFileInfo().getLength());
    }
    mMetastore.putDoraMeta(path, meta);
  }

  /**
   * Removes meta of a file from the meta store.
   * @param path the full ufs path
   * @return the removed file meta, if exists
   */
  public Optional<FileStatus> removeFromMetaStore(String path) {
    Optional<FileStatus> status = mMetastore.getDoraMeta(path);
    if (status.isPresent()) {
      mMetastore.removeDoraMeta(path);
      invalidateCachedFile(path, status.get().getFileInfo().getLength());
    }
    return status;
  }

  private void invalidateCachedFile(String path, long length) {
    FileId fileId = FileId.of(AlluxioURI.hash(path));
    mCacheManager.deleteFile(fileId.toString());
    for (PageId page: mCacheManager.getCachedPageIdsByFileId(fileId.toString(), length)) {
      mCacheManager.delete(page);
    }
  }

  @VisibleForTesting
  void setPopulateMetadataFingerprint(boolean value) {
    mPopulateMetadataFingerprint = value;
  }
}
