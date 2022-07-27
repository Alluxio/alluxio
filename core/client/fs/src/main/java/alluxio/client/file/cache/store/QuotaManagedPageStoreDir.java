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

import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.resource.LockResource;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
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
  public boolean putPage(PageInfo pageInfo) {
    mEvictor.updateOnPut(pageInfo.getPageId());
    try (LockResource lock = new LockResource(mFileIdSetLock.writeLock())) {
      mFileIdSet.add(pageInfo.getPageId().getFileId());
    }
    mBytesUsed.addAndGet(pageInfo.getPageSize());
    return true;
  }

  @Override
  public long deletePage(PageInfo pageInfo) {
    mEvictor.updateOnDelete(pageInfo.getPageId());
    return mBytesUsed.addAndGet(-pageInfo.getPageSize());
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
