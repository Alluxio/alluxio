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

package alluxio.client.file.cache.store;

import static com.google.common.base.Preconditions.checkState;

import alluxio.client.file.cache.CacheUsage;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.resource.LockResource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

abstract class QuotaManagedPageStoreDir implements PageStoreDir {

  private final ReentrantReadWriteLock mFileIdSetLock = new ReentrantReadWriteLock();
  @GuardedBy("mFileIdSetLock")
  private final Set<String> mFileIdSet = new HashSet<>();

  private final ReentrantReadWriteLock mTempFileIdSetLock = new ReentrantReadWriteLock();
  @GuardedBy("mTempFileIdSetLock")
  private final Set<String> mTempFileIdSet = new HashSet<>();

  private final Map<String, List<PageInfo>> mTempFileToPageInfoListMap = new ConcurrentHashMap<>();

  private final Path mRootPath;
  private final long mCapacityBytes;
  private final AtomicLong mBytesUsed = new AtomicLong(0);

  private final CacheEvictor mEvictor;

  QuotaManagedPageStoreDir(Path rootPath, long capacityBytes, CacheEvictor evictor) {
    mRootPath = rootPath;
    mCapacityBytes = capacityBytes;
    mEvictor = evictor;
  }

  @Override
  public Path getRootPath() {
    return mRootPath;
  }

  @Override
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  @Override
  public long getCachedBytes() {
    return mBytesUsed.get();
  }

  @Override
  public void putPage(PageInfo pageInfo) {
    mEvictor.updateOnPut(pageInfo.getPageId());
    try (LockResource lock = new LockResource(mFileIdSetLock.writeLock())) {
      mFileIdSet.add(pageInfo.getPageId().getFileId());
    }
    mBytesUsed.addAndGet(pageInfo.getPageSize());
  }

  @Override
  public void putTempPage(PageInfo pageInfo) {
    mTempFileToPageInfoListMap.computeIfAbsent(pageInfo.getPageId().getFileId(),
        tempFileId -> new ArrayList<>()).add(pageInfo);
    try (LockResource lock = new LockResource(mTempFileIdSetLock.readLock())) {
      mTempFileIdSet.add(pageInfo.getPageId().getFileId());
    }
    mBytesUsed.addAndGet(pageInfo.getPageSize());
  }

  @Override
  public long deletePage(PageInfo pageInfo) {
    mEvictor.updateOnDelete(pageInfo.getPageId());
    return mBytesUsed.addAndGet(-pageInfo.getPageSize());
  }

  @Override
  public void deleteTempPage(PageInfo pageInfo) {
    String fileId = pageInfo.getPageId().getFileId();
    if (mTempFileToPageInfoListMap.containsKey(fileId)) {
      List<PageInfo> pageInfoList =
          mTempFileToPageInfoListMap.get(fileId);
      if (pageInfoList != null && pageInfoList.contains(pageInfo)) {
        pageInfoList.remove(pageInfo);
        if (pageInfoList.isEmpty()) {
          mTempFileToPageInfoListMap.remove(fileId);
          try (LockResource lock = new LockResource(mTempFileIdSetLock.readLock())) {
            mTempFileIdSet.remove(fileId);
          }
        }
      }
    }
    mBytesUsed.addAndGet(-pageInfo.getPageSize());
  }

  @Override
  public boolean putTempFile(String fileId) {
    try (LockResource lock = new LockResource(mTempFileIdSetLock.writeLock())) {
      return mTempFileIdSet.add(fileId);
    }
  }

  @Override
  public boolean reserve(long bytes) {
    long previousBytesUsed;
    do {
      previousBytesUsed = mBytesUsed.get();
      if (previousBytesUsed + bytes > mCapacityBytes) {
        return false;
      }
    } while (!mBytesUsed.compareAndSet(previousBytesUsed, previousBytesUsed + bytes));
    return true;
  }

  @Override
  public long release(long bytes) {
    return mBytesUsed.addAndGet(-bytes);
  }

  @Override
  public boolean hasFile(String fileId) {
    try (LockResource lock = new LockResource(mFileIdSetLock.readLock())) {
      return mFileIdSet.contains(fileId);
    }
  }

  @Override
  public boolean hasTempFile(String fileId) {
    try (LockResource lock = new LockResource(mTempFileIdSetLock.readLock())) {
      return mTempFileIdSet.contains(fileId);
    }
  }

  @Override
  public CacheEvictor getEvictor() {
    return mEvictor;
  }

  @Override
  public void close() {
    try {
      getPageStore().close();
      mBytesUsed.set(0);
    } catch (Exception e) {
      throw new RuntimeException("Close page store failed for dir " + getRootPath().toString(), e);
    }
  }

  @Override
  public void commit(String fileId, String newFileId) throws IOException {
    try (LockResource tempFileIdSetlock = new LockResource(mTempFileIdSetLock.writeLock());
        LockResource fileIdSetlock = new LockResource(mFileIdSetLock.writeLock())) {
      checkState(mTempFileIdSet.contains(fileId), "temp file does not exist " + fileId);
      // We need a new interface for {@PageStore} interface to remove all pages of a cached file,
      // and remove the fileId from this mFileIdSet. See {@DoraWorker#invalidateCachedFile}.
      // Currently, invalidated file is still in this map.
      //checkState(!mFileIdSet.contains(newFileId), "file already committed " + newFileId);
      getPageStore().commit(fileId, newFileId);
      mTempFileIdSet.remove(fileId);
      mFileIdSet.add(newFileId);

      mTempFileToPageInfoListMap.get(fileId)
          .forEach(pageInfo -> mEvictor.updateOnPut(pageInfo.getPageId()));
      mTempFileToPageInfoListMap.remove(fileId);
    }
  }

  @Override
  public void abort(String fileId) throws IOException {
    try (LockResource tempFileIdSetlock = new LockResource(mTempFileIdSetLock.writeLock())) {
      checkState(mTempFileIdSet.contains(fileId), "temp file does not exist " + fileId);
      getPageStore().abort(fileId);
      mTempFileIdSet.remove(fileId);
    }
  }

  /**
   * Generic implementation of cache usage stats.
   * Subclasses may need to override the individual cache stat to reflect their own logic
   * of usage accounting.
   */
  class Usage implements CacheUsage {
    @Override
    public long used() {
      return getCachedBytes();
    }

    @Override
    public long available() {
      // TODO(bowen): take reserved bytes into account
      return getCapacityBytes() - getCachedBytes();
    }

    @Override
    public long capacity() {
      return getCapacityBytes();
    }

    /**
     * This generic implementation assumes the directory does not support finer-grained
     * stats partitioning.
     *
     * @param partition how to partition the cache
     * @return always empty
     */
    @Override
    public Optional<CacheUsage> partitionedBy(PartitionDescriptor<?> partition) {
      return Optional.empty();
    }
  }
}
