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

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
import alluxio.resource.LockResource;

import com.google.common.base.Preconditions;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

public abstract class QuotaManagedPageStoreDir implements PageStoreDir {

  private final ReentrantReadWriteLock mFileIdSetLock = new ReentrantReadWriteLock();
  @GuardedBy("mFileIdSetLock")
  private final Set<String> mFileIdSet = new HashSet<>();

  private final Path mRootPath;
  private final long mCapacityBytes;
  private final AtomicLong mBytesUsed = new AtomicLong(0);

  private final CacheEvictor mEvictor;

  private final boolean mQuotaEnabled;

  QuotaManagedPageStoreDir(Path rootPath, long capacityBytes, CacheEvictor evictor,
      boolean quotaEnabled) {
    mRootPath = rootPath;
    mCapacityBytes = capacityBytes;
    mEvictor = evictor;
    mQuotaEnabled = quotaEnabled;
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
  public boolean putPage(PageReservation pageReservation) {
    checkValidReservation(pageReservation);
    PageInfo pageInfo = pageReservation.getPageInfo();
    mEvictor.updateOnPut(pageInfo.getPageId());
    try (LockResource lock = new LockResource(mFileIdSetLock.writeLock())) {
      mFileIdSet.add(pageInfo.getPageId().getFileId());
    }
    mBytesUsed.addAndGet(pageInfo.getPageSize());
    pageReservation.setExpired();
    return true;
  }

  private void checkValidReservation(PageReservation pageReservation) {
    Preconditions.checkArgument(pageReservation.getDestDir() == this,
        "reservation is obtained from another store dir");
    Preconditions.checkArgument(!pageReservation.isExpired());
  }

  @Override
  public long deletePage(PageInfo pageInfo) {
    mEvictor.updateOnDelete(pageInfo.getPageId());
    return mBytesUsed.addAndGet(-pageInfo.getPageSize());
  }

  @Override
  public boolean putTempFile(String fileId) {
    try (LockResource lock = new LockResource(mFileIdSetLock.writeLock())) {
      return mFileIdSet.add(fileId);
    }
  }

  @Override
  public Optional<PageReservation> reserve(PageInfo pageInfo, ReservationOptions options) {
    if (!mQuotaEnabled) {
      return Optional.of(new PageReservation(pageInfo, this));
    }
    PageReservation reservation = new PageReservation(pageInfo, this);
    if (options.mForceEviction) {
      PageId victim = options.mEvictor.evict();
      reservation.setVictimPage(victim);
      return Optional.of(reservation);
    }
    long bytes = pageInfo.getPageSize();
    long previousBytesUsed;
    boolean evict = false;
    do {
      previousBytesUsed = mBytesUsed.get();
      if (previousBytesUsed + bytes > mCapacityBytes) {
        if (options.isEvictionEnabled()) {
          evict = true;
        }
      }
    } while (!mBytesUsed.compareAndSet(previousBytesUsed, previousBytesUsed + bytes));
    if (evict) {
      PageId victim = options.mEvictor.evict();
      reservation.setVictimPage(victim);
      return Optional.of(reservation);
    }
    return Optional.empty();
  }

  @Override
  public long release(PageReservation pageReservation) {
    checkValidReservation(pageReservation);
    long pageSize = pageReservation.getPageInfo().getPageSize();
    pageReservation.setExpired();
    return mBytesUsed.addAndGet(-pageSize);
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

  public static class ReservationOptions {

    private boolean mForceEviction;
    private CacheScope mEvictionScope;
    private CacheQuota mQuota;

    private CacheEvictor mEvictor;

    public ReservationOptions enableEviction(CacheScope scopeToEvict, CacheQuota quota,
        CacheEvictor evictor, boolean force) {
      mEvictionScope = scopeToEvict;
      mQuota = quota;
      mEvictor = evictor;
      mForceEviction = force;
      return this;
    }

    public boolean isEvictionEnabled() {
      return mForceEviction || mEvictor != null;
    }
  }

  public static class PageReservation {
    private final PageInfo mPageInfo;
    private final PageStoreDir mDestDir;
    private boolean mExpired = false;

    private Optional<PageId> mVictimPage = Optional.empty();

    private PageReservation(PageInfo pageInfo, PageStoreDir dir) {
      mPageInfo = pageInfo;
      mDestDir = dir;
    }

    public PageInfo getPageInfo() {
      return mPageInfo;
    }

    public PageStoreDir getDestDir() {
      return mDestDir;
    }

    private void setExpired() {
      mExpired = true;
    }

    public boolean isExpired() {
      return mExpired;
    }

    private void setVictimPage(PageId victimPage) {
      mVictimPage = Optional.of(victimPage);
    }

    public Optional<PageId> getVictimPage() {
      return mVictimPage;
    }
  }
}
