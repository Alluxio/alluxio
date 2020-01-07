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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
public class LocalCacheManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheManager.class);
  /** Number of page locks to strip. */
  private static int PAGE_LOCK_SIZE = 256;

  private final int mPageSize;
  private final int mCacheSize;
  private final CacheEvictor mEvictor;
  /** A readwrite lock pool to guard individual pages based on striping. */
  private final ReadWriteLock[] mPageLocks = new ReentrantReadWriteLock[PAGE_LOCK_SIZE];
  private final PageStore mPageStore;
  /** A readwrite lock to guard metadata operations */
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
    for (int i = 0; i < PAGE_LOCK_SIZE; i ++) {
      mPageLocks[i] = new ReentrantReadWriteLock();
    }
    mFsContext = fsContext;
    mMetaStore = metaStore;
    mPageStore = pageStore;
    mEvictor = evictor;
    mPageSize = mFsContext.getClusterConf().getInt(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    mCacheSize = mFsContext.getClusterConf().getInt(PropertyKey.USER_CLIENT_CACHE_SIZE);
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
    return mPageLocks[(int) (fileId + pageIndex) % PAGE_LOCK_SIZE];
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

  /**
   * Writes a new page from a source channel with best effort.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @param src source channel to read this new page
   * @throws IOException
   * @return the number of bytes written
   */
  public int put(long fileId, long pageIndex, ReadableByteChannel src) throws IOException {
    long victimFileId = 0, victimPageIndex = 0;

    ReadWriteLock pageLock = getPageLock(fileId, pageIndex);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      boolean alreadyCached, needEvict = false;
      try (LockResource r2 = new LockResource(mMetaLock.writeLock())) {
        alreadyCached = mMetaStore.hasPage(fileId, pageIndex);
        if (!alreadyCached) {
          needEvict = (mPageSize + mPageStore.size()) > mCacheSize;
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
        mPageStore.delete(fileId, pageIndex);
        return mPageStore.put(fileId, pageIndex, src);
      } else if (!needEvict) {
        return mPageStore.put(fileId, pageIndex, src);
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
          return 0;
        }
        if (!mMetaStore.hasPage(victimFileId, victimPageIndex)) {
          LOG.warn("fileId {} pageIndex {} is already evicted by a racing thread",
              fileId, pageIndex);
          return 0;
        }
        mMetaStore.removePage(victimFileId, victimPageIndex);
        mMetaStore.addPage(fileId, pageIndex);
      }
      mPageStore.delete(victimFileId, victimPageIndex);
      return mPageStore.put(fileId, pageIndex, src);
    }
  }

  /**
   * Reads a page to the destination channel.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @param dst destination channel to read this new page
   * @return the number of bytes read
   */
  public int get(long fileId, long pageIndex, WritableByteChannel dst) throws IOException {
    return get(fileId, pageIndex, 0, mPageSize, dst);
  }

  /**
   * Reads a part of a page to the destination channel.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @param pageOffset offset into the page
   * @param length length to read
   * @param dst destination channel to read this new page
   * @return the number of bytes read
   */
  public int get(long fileId, long pageIndex, int pageOffset, int length, WritableByteChannel dst)
      throws IOException {
    Preconditions.checkArgument(pageOffset + length <= mPageSize,
        "Read exceeds page boundary: offset=%s length=%s, size=%s", pageOffset, length, mPageSize);
    int ret = 0;
    boolean hasPage;
    ReadWriteLock pageLock = getPageLock(fileId, pageIndex);
    try (LockResource r = new LockResource(pageLock.readLock())) {
      try (LockResource r2 = new LockResource(mMetaLock.readLock())) {
        hasPage = mMetaStore.hasPage(fileId, pageIndex);
      }
      if (!hasPage) {
        return 0;
      }
      if (pageOffset == 0) {
        ret = mPageStore.get(fileId, pageIndex, dst);
      } else {
        //
        // TODO: Extend page store API to get offset to avoid copy to use something like
        // mPageStore.get(fileId, pageIndex, dst, pageOffset, length);
        //
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (WritableByteChannel bufChan = Channels.newChannel(buf)) {
          ret = mPageStore.get(fileId, pageIndex, bufChan) - pageOffset;
        }
        if (ret <= 0) {
          return 0;
        }
        byte[] array = Arrays.copyOfRange(buf.toByteArray(), pageOffset, pageOffset + ret);
        try (ReadableByteChannel bufChan = Channels.newChannel(new ByteArrayInputStream(array))) {
          BufferUtils.fastCopy(bufChan, dst);
        }
      }
      mEvictor.updateOnGet(fileId, pageIndex);
      return ret;
    }
  }

  /**
   * Deletes a page from the cache.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @return if the page was deleted
   * @throws IOException
   */
  public boolean delete(long fileId, long pageIndex) throws IOException {
    ReadWriteLock pageLock = getPageLock(fileId, pageIndex);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      boolean removed;
      try (LockResource r1 = new LockResource(mMetaLock.writeLock())) {
        removed = mMetaStore.removePage(fileId, pageIndex);
      }
      return removed && mPageStore.delete(fileId, pageIndex);
    }
  }

}
