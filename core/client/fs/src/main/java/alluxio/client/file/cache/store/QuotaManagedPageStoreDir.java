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

import static java.util.Objects.requireNonNull;

import alluxio.client.file.cache.FileInfo;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.resource.LockResource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

abstract class QuotaManagedPageStoreDir implements PageStoreDir {

  private final ReentrantReadWriteLock mFileMapLock = new ReentrantReadWriteLock();
  @GuardedBy("mFileIdSetLock")
  private final Map<String, FileInfo> mFileMap = new HashMap<>();

  private final ReentrantReadWriteLock mTempFileMapLock = new ReentrantReadWriteLock();
  @GuardedBy("mTempFileIdSetLock")
  private final Map<String, FileInfo> mTempFileMap = new HashMap<>();

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
    mBytesUsed.addAndGet(pageInfo.getPageSize());
    String fileId = pageInfo.getPageId().getFileId();
    try (LockResource readLock = new LockResource(mFileMapLock.readLock())) {
      FileInfo fileInfo = mFileMap.get(fileId);
      if (fileInfo != null) {
        fileInfo.increaseSize(pageInfo.getPageSize());
        fileInfo.increasePageCount();
        return;
      }
    }
    try (LockResource writeLock = new LockResource(mFileMapLock.writeLock())) {
      FileInfo fileInfo = mFileMap.get(fileId);
      if (fileInfo != null) {
        fileInfo.increaseSize(pageInfo.getPageSize());
        fileInfo.increasePageCount();
      } else {
        fileInfo = new FileInfo(fileId, "", pageInfo.getPageSize());
        fileInfo.decreasePageCount();
        mFileMap.put(fileId, fileInfo);
      }
    }
  }

  @Override
  public void putTempPage(PageInfo pageInfo) {
    try (LockResource lock = new LockResource(mTempFileMapLock.readLock())) {
      FileInfo fileInfo = requireNonNull(mTempFileMap.get(pageInfo.getPageId().getFileId()),
          "Temp file does not exist " + pageInfo.getPageId());
      fileInfo.increaseSize(pageInfo.getPageSize());
    }
  }

  @Override
  public long deletePage(PageInfo pageInfo) {
    mEvictor.updateOnDelete(pageInfo.getPageId());
    String fileId = pageInfo.getPageId().getFileId();
    try (LockResource readLock = new LockResource(mFileMapLock.readLock())) {
      FileInfo fileInfo = mFileMap.get(fileId);
      if (fileInfo != null) {
        fileInfo.increaseSize(-pageInfo.getPageSize());
        if (fileInfo.decreasePageCount() > 0) {
          return mBytesUsed.addAndGet(-pageInfo.getPageSize());
        }
      }
    }
    try (LockResource writeLock = new LockResource(mFileMapLock.writeLock())) {
      FileInfo fileInfo = mFileMap.get(fileId);
      if (fileInfo != null && fileInfo.getPageCount() == 0) {
        mFileMap.remove(fileId);
      }
    }
    return mBytesUsed.addAndGet(-pageInfo.getPageSize());
  }

  @Override
  public boolean putTempFile(String fileId) {
    try (LockResource lock = new LockResource(mTempFileMapLock.writeLock())) {
      if (!mTempFileMap.containsKey(fileId)) {
        mTempFileMap.put(fileId, new FileInfo(fileId, "", 0));
        return true;
      }
      return false;
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
    try (LockResource lock = new LockResource(mFileMapLock.readLock())) {
      return mFileMap.containsKey(fileId);
    }
  }

  @Override
  public boolean hasTempFile(String fileId) {
    try (LockResource lock = new LockResource(mTempFileMapLock.readLock())) {
      return mTempFileMap.containsKey(fileId);
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
}
