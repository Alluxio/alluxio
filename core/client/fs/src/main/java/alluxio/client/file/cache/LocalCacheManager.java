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

package alluxio.client.file.cache;

import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.resource.LockResource;
import alluxio.util.io.BufferUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A class to manage cached pages. This class coordinates different components to respond for
 * thread-safety and operate cache replacement policies.
 *
 * Lock hierarchy in this class: All operations must follow this order to operate on pages:
 * <ul>
 * <li>1. Acquire page lock</li>
 * <li>2. Acquire metastore lock mMetaLock</li>
 * <li>3. Release metastore lock mMetaLock</li>
 * <li>4. Release page lock</li>
 * </ul>
 */
@ThreadSafe
public class LocalCacheManager implements CacheManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheManager.class);

  private static final int LOCK_SIZE = 1024;
  private final int mPageSize;
  private final long mCacheSize;
  private final CacheEvictor mEvictor;
  /** A readwrite lock pool to guard individual pages based on striping. */
  private final ReadWriteLock[] mPageLocks = new ReentrantReadWriteLock[LOCK_SIZE];
  private final PageStore mPageStore;
  /** A readwrite lock to guard metadata operations. */
  private final ReadWriteLock mMetaLock = new ReentrantReadWriteLock();
  @GuardedBy("mMetaLock")
  private final MetaStore mMetaStore;
  private final FileSystemContext mFsContext;

  /**
   * @param fsContext filesystem context
   */
  public LocalCacheManager(FileSystemContext fsContext) {
    this(fsContext, new MetaStore(), PageStore.create(), CacheEvictor.create());
  }

  /**
   * @param fsContext filesystem context
   */
  @VisibleForTesting
  LocalCacheManager(FileSystemContext fsContext, MetaStore metaStore,
                    PageStore pageStore, CacheEvictor evictor) {
    mFsContext = fsContext;
    mMetaStore = metaStore;
    mPageStore = pageStore;
    mEvictor = evictor;
    mPageSize = (int) mFsContext.getClusterConf().getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    mCacheSize = mFsContext.getClusterConf().getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE) / mPageSize;
    for (int i = 0; i < LOCK_SIZE; i++) {
      mPageLocks[i] = new ReentrantReadWriteLock();
    }
  }

  /**
   * Gets the lock for a particular page. Note that multiple pages may share the same lock as lock
   * striping is used to reduce resource overhead for locks.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @return the corresponding page lock
   */
  private ReadWriteLock getPageLock(long fileId, long pageIndex) {
    return mPageLocks[(int) (fileId + pageIndex) % LOCK_SIZE];
  }

  /**
   * Gets a pair of locks to operate two given pages. One MUST acquire the first lock followed by
   * the second lock.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @param fileId2 file identifier
   * @param pageIndex2 index of the page within the file
   * @return the corresponding page lock pair
   */
  private Pair<ReadWriteLock, ReadWriteLock> getPageLockPair(long fileId, long pageIndex,
      long fileId2, long pageIndex2) {
    if (fileId + pageIndex < fileId2 + pageIndex2) {
      return new Pair<>(getPageLock(fileId, pageIndex), getPageLock(fileId2, pageIndex2));
    } else {
      return new Pair<>(getPageLock(fileId2, pageIndex2), getPageLock(fileId, pageIndex));
    }
  }

  @Override
  public void put(long fileId, long pageIndex, byte[] page) throws IOException {
    long victimFileId = 0;
    long victimPageIndex = 0;

    ReadWriteLock pageLock = getPageLock(fileId, pageIndex);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      boolean alreadyCached;
      boolean needEvict = false;
      try (LockResource r2 = new LockResource(mMetaLock.writeLock())) {
        alreadyCached = mMetaStore.hasPage(fileId, pageIndex);
        if (!alreadyCached) {
          needEvict = mPageStore.size() + 1 > mCacheSize;
          if (needEvict) {
            Pair<Long, Long> victim = mEvictor.evict();
            victimFileId = victim.getFirst();
            victimPageIndex = victim.getSecond();
          } else {
            mMetaStore.addPage(fileId, pageIndex);
          }
        }
      }
      if (alreadyCached) {
        try {
          mPageStore.delete(fileId, pageIndex);
        } catch (PageNotFoundException e) {
          // this should never happen with proper locking
          LOG.error("failed to delete page {} {} from page store", fileId, pageIndex, e);
        }
        mEvictor.updateOnPut(fileId, pageIndex);
        mPageStore.put(fileId, pageIndex, page);
      } else if (!needEvict) {
        mEvictor.updateOnPut(fileId, pageIndex);
        mPageStore.put(fileId, pageIndex, page);
      }
    }

    Pair<ReadWriteLock, ReadWriteLock> pageLockPair =
        getPageLockPair(fileId, pageIndex, victimFileId, victimPageIndex);
    try (LockResource r1 = new LockResource(pageLockPair.getFirst().writeLock());
        LockResource r2 = new LockResource(pageLockPair.getSecond().writeLock())) {
      try (LockResource r3 = new LockResource(mMetaLock.writeLock())) {
        if (mMetaStore.hasPage(fileId, pageIndex)) {
          LOG.warn("fileId {} pageIndex {} is already inserted by a racing thread",
              fileId, pageIndex);
          return;
        }
        if (!mMetaStore.hasPage(victimFileId, victimPageIndex)) {
          LOG.warn("fileId {} pageIndex {} is already evicted by a racing thread",
              fileId, pageIndex);
          return;
        }
        try {
          mMetaStore.removePage(victimFileId, victimPageIndex);
        } catch (PageNotFoundException e) {
          // this should never happen with proper locking
          LOG.error("failed to remove page {} {} from meta store",
              victimFileId, victimPageIndex, e);
        }
        mEvictor.updateOnDelete(victimFileId, victimPageIndex);
        mMetaStore.addPage(fileId, pageIndex);
        mEvictor.updateOnPut(fileId, pageIndex);
      }
      try {
        mPageStore.delete(victimFileId, victimPageIndex);
      } catch (PageNotFoundException e) {
        // this should never happen with proper locking
        LOG.error("failed to delete page {} {} from page store", victimFileId, victimPageIndex, e);
      }
      mPageStore.put(fileId, pageIndex, page);
    }
  }

  @Override
  public ReadableByteChannel get(long fileId, long pageIndex) throws IOException,
      PageNotFoundException {
    return get(fileId, pageIndex, 0, mPageSize);
  }

  @Override
  public ReadableByteChannel get(long fileId, long pageIndex, int pageOffset, int length)
      throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset + length <= mPageSize,
        "Read exceeds page boundary: offset=%s length=%s, size=%s", pageOffset, length, mPageSize);
    ReadableByteChannel ret;
    boolean hasPage;
    ReadWriteLock pageLock = getPageLock(fileId, pageIndex);
    try (LockResource r = new LockResource(pageLock.readLock())) {
      try (LockResource r2 = new LockResource(mMetaLock.readLock())) {
        hasPage = mMetaStore.hasPage(fileId, pageIndex);
      }
      if (!hasPage) {
        throw new PageNotFoundException(
            String.format("Page (%d, %d) could not be found", fileId, pageIndex));
      }
      if (pageOffset == 0) {
        ret = mPageStore.get(fileId, pageIndex);
      } else {
        //
        // TODO(feng): Extend page store API to get offset to avoid copy to use something like
        // mPageStore.get(fileId, pageIndex, pageOffset, length);
        //
        ByteBuffer buf = ByteBuffer.allocate(mPageSize);
        int bytesRead;
        try (ReadableByteChannel pageChannel = mPageStore.get(fileId, pageIndex)) {
          bytesRead = pageChannel.read(buf);
        }
        if (bytesRead < pageOffset) {
          return null;
        }
        buf.flip();
        buf.position(pageOffset);
        buf.limit(Math.min(bytesRead, pageOffset + length));
        ret = Channels.newChannel(new ByteBufferInputStream(buf));
      }
      mEvictor.updateOnGet(fileId, pageIndex);
      return ret;
    }
  }

  @Override
  public void delete(long fileId, long pageIndex) throws IOException, PageNotFoundException {
    ReadWriteLock pageLock = getPageLock(fileId, pageIndex);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      try (LockResource r1 = new LockResource(mMetaLock.writeLock())) {
        mMetaStore.removePage(fileId, pageIndex);
      }
      mPageStore.delete(fileId, pageIndex);
    }
  }
}
