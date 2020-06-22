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
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.resource.LockResource;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The {@link MemoryPageStore} is an implementation of {@link PageStore} which stores all pages in
 * Java direct memory.
 */
@ThreadSafe
public class MemoryPageStore implements PageStore {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryPageStore.class);

  private class PageEntry {
    int mOffset;
    int mLength;

    public PageEntry(int offset, int length) {
      mOffset = offset;
      mLength = length;
    }
  }

  private final ByteBuffer mBuffer;
  private final int mCapacity;
  private final int mPageSize;
  private final ReadWriteLock mLock = new ReentrantReadWriteLock();
  /** A map from pageId to its offset in the buffer. */
  @GuardedBy("mLock")
  private final Map<PageId, PageEntry> mPageEntryMap = new HashMap<>();
  /** A set of available offsets in the buffer to store new pages. */
  @GuardedBy("mLock")
  private final Set<Integer> mAvailableIndexes = new HashSet<>();

  /**
   * Creates a new instance of {@link MemoryPageStore}.
   *
   * @param options options for the memory page store
   */
  public MemoryPageStore(MemoryPageStoreOptions options) {
    Preconditions.checkArgument(options.getPageSize() <= Integer.MAX_VALUE,
        "page size must be smaller than %s", Integer.MAX_VALUE);
    Preconditions.checkArgument(options.getCacheSize() <= Integer.MAX_VALUE,
        "cache size must be smaller than %s", Integer.MAX_VALUE);
    mPageSize = (int) options.getPageSize();
    mCapacity = (int) options.getCacheSize();
    mBuffer = ByteBuffer.allocateDirect(mCapacity);
    for (int offset = 0; offset < mCapacity; offset += mPageSize) {
      mAvailableIndexes.add(offset);
    }
  }

  @Override
  public void put(PageId pageId, byte[] page) throws IOException {
    Preconditions.checkArgument(page.length <= mPageSize, "page size %s exceeds than %s",
        page.length, mPageSize);
    try (LockResource r1 = new LockResource(mLock.writeLock())) {
      try {
        if (mPageEntryMap.containsKey(pageId)) {
          delete(pageId);
          LOG.warn("Overwriting page {}", pageId);
        }
      } catch (PageNotFoundException e) {
        // We shall never reach here
        throw new IOException("Failed to put " + pageId + ": inconsistent state");
      }
      if (mAvailableIndexes.isEmpty()) {
        throw new IOException("Failed to put " + pageId + ": store is full");
      }
      int offset = mAvailableIndexes.iterator().next();
      mBuffer.position(offset);
      mBuffer.put(page);
      mAvailableIndexes.remove(offset);
      mPageEntryMap.put(pageId, new PageEntry(offset, page.length));
    }
  }

  @Override
  public ReadableByteChannel get(PageId pageId, int pageOffset)
      throws IOException, PageNotFoundException {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public int get(PageId pageId, int offsetInPage, int bytesToRead, byte[] buffer,
      int offsetInBuffer) throws PageNotFoundException {
    try (LockResource r1 = new LockResource(mLock.readLock())) {
      if (!mPageEntryMap.containsKey(pageId)) {
        throw new PageNotFoundException("Failed to get " + pageId + ": pageId not found");
      }
      PageEntry entry = mPageEntryMap.get(pageId);
      Preconditions.checkArgument(offsetInPage <= entry.mLength,
          "page offset %s exceeded page size %s", offsetInPage, entry.mLength);
      int offset = entry.mOffset;
      mBuffer.position(offset + offsetInPage);
      mBuffer.get(buffer, offsetInBuffer, bytesToRead);
      return bytesToRead;
    }
  }

  @Override
  public void delete(PageId pageId) throws PageNotFoundException {
    try (LockResource r1 = new LockResource(mLock.writeLock())) {
      if (!mPageEntryMap.containsKey(pageId)) {
        throw new PageNotFoundException("Failed to delete " + pageId + ": pageId not found");
      }
      int offset = mPageEntryMap.remove(pageId).mOffset;
      mAvailableIndexes.add(offset);
    }
  }

  @Override
  public Stream<PageInfo> getPages() {
    return mPageEntryMap.entrySet().stream()
        .map(e -> new PageInfo(e.getKey(), e.getValue().mLength));
  }

  @Override
  public long getCacheSize() {
    return mCapacity;
  }

  @Override
  public void close() throws Exception {
    BufferUtils.cleanDirectBuffer(mBuffer);
  }
}
