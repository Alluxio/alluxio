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

import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.conf.AlluxioConfiguration;
import alluxio.proto.client.Cache;
import alluxio.resource.LockResource;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

abstract class QuotaPageStoreDir implements PageStoreDir {

  private final ReentrantReadWriteLock mBlockPageMapLock = new ReentrantReadWriteLock();
  @GuardedBy("mFileIdSetLock")
  private final Set<String> mFileIdSet = new HashSet<>();

  private final Path mRootPath;
  private final long mCapacity;
  private final AtomicLong mBytesUsed = new AtomicLong(0);

  private final CacheEvictor mEvictor;

  QuotaPageStoreDir(AlluxioConfiguration conf, Path rootPath, long capacity) {
    mRootPath = rootPath;
    mCapacity = capacity;
    mEvictor = CacheEvictor.create(conf);
  }

  @Override
  public Path getRootPath() {
    return mRootPath;
  }

  @Override
  public long getCapacity() {
    return mCapacity;
  }

  @Override
  public long getCachedBytes() {
    return 0;
  }

  @Override
  public boolean addFileToDir(String fileId) {
    try (LockResource lock = new LockResource(mBlockPageMapLock.writeLock())) {
      return mFileIdSet.add(fileId);
    }
  }

  @Override
  public boolean reserveSpace(int bytes) {
    long previousBytesUsed;
    do {
      previousBytesUsed = mBytesUsed.get();
      if (previousBytesUsed + bytes > mCapacity) {
        return false;
      }
    } while (!mBytesUsed.compareAndSet(previousBytesUsed, previousBytesUsed + bytes));
    return true;
  }

  @Override
  public long releaseSpace(int bytes) {
    return mBytesUsed.addAndGet(bytes);
  }

  @Override
  public boolean hasFile(String fileId) {
    try (LockResource lock = new LockResource(mBlockPageMapLock.readLock())) {
      return mFileIdSet.contains(fileId);
    }
  }

  @Override
  public CacheEvictor getEvictor() {
    return mEvictor;
  }
}
