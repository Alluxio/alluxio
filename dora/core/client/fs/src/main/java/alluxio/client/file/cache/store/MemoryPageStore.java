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
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.file.ReadTargetBuffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link MemoryPageStore} is an implementation of {@link PageStore} which
 * stores all pages in memory.
 */
@NotThreadSafe
public class MemoryPageStore implements PageStore {

  private final PagePool mPagePool;

  private ConcurrentHashMap<PageId, MemPage> mPageStoreMap = new ConcurrentHashMap<>();

  /**
   * Constructor of MemoryPageStore.
   * @param pageSize page size
   */
  public MemoryPageStore(int pageSize) {
    mPagePool = new PagePool(pageSize);
  }

  @Override
  public void put(PageId pageId, ByteBuffer page, boolean isTemporary) throws IOException {
    //TODO(beinan): support temp page for memory page store
    PageId pageKey = getKeyFromPageId(pageId);
    try {
      MemPage pageCopy = mPagePool.acquire(page.remaining());
      page.get(pageCopy.getPage(), 0, pageCopy.getPageLength());
      mPageStoreMap.put(pageKey, pageCopy);
    } catch (Exception e) {
      throw new IOException("Failed to put cached data in memory for page " + pageId);
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, ReadTargetBuffer target,
      boolean isTemporary) throws IOException, PageNotFoundException {
    Preconditions.checkArgument(target != null, "buffer is null");
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    PageId pageKey = getKeyFromPageId(pageId);
    if (!mPageStoreMap.containsKey(pageKey)) {
      throw new PageNotFoundException(pageId.getFileId() + "_" + pageId.getPageIndex());
    }
    MemPage page = mPageStoreMap.get(pageKey);
    Preconditions.checkArgument(pageOffset <= page.getPageLength(),
        "page offset %s exceeded page size %s", pageOffset, page.getPageLength());
    int bytesLeft = (int) Math.min(page.getPageLength() - pageOffset, target.remaining());
    bytesLeft = Math.min(bytesLeft, bytesToRead);
    target.writeBytes(page.getPage(), pageOffset, bytesLeft);
    return bytesLeft;
  }

  @Override
  public void delete(PageId pageId) throws IOException, PageNotFoundException {
    PageId pageKey = getKeyFromPageId(pageId);
    if (!mPageStoreMap.containsKey(pageKey)) {
      throw new PageNotFoundException(pageId.getFileId() + "_" + pageId.getPageIndex());
    }
    mPagePool.release(mPageStoreMap.get(pageKey));
    mPageStoreMap.remove(pageKey);
  }

  @Override
  public void commit(String fileId, String newFileId) throws IOException {
    // noop because the pages are all in memory, there is no underlying storage to commit to
  }

  /**
   * @param pageId page Id
   * @return the key to this page
   */
  @VisibleForTesting
  public PageId getKeyFromPageId(PageId pageId) {
    // TODO(feng): encode fileId with URLEncoder to escape invalid characters for file name
    // Key is : PageId
    return pageId;
  }

  @Override
  public void close() {
    mPageStoreMap.clear();
    mPageStoreMap = null;
    mPagePool.close();
  }

  /**
   *
   */
  public void reset() {
    mPageStoreMap.clear();
  }

  private static class MemPage {
    private final byte[] mPage;
    private int mPageLength;

    public MemPage(byte[] page, int pageLength) {
      mPage = page;
      mPageLength = pageLength;
    }

    public byte[] getPage() {
      return mPage;
    }

    public int getPageLength() {
      return mPageLength;
    }

    public void setPageLength(int pageLength) {
      mPageLength = pageLength;
    }
  }

  private static class PagePool implements Closeable {
    private final int mPageSize;
    private final LinkedList<MemPage> mPool = new LinkedList<>();

    public PagePool(int pageSize) {
      mPageSize = pageSize;
    }

    public MemPage acquire(int pageLength) {
      synchronized (mPool) {
        if (!mPool.isEmpty()) {
          MemPage page = mPool.pop();
          page.setPageLength(pageLength);
          return page;
        }
      }
      return new MemPage(new byte[mPageSize], pageLength);
    }

    public void release(MemPage page) {
      synchronized (mPool) {
        mPool.push(page);
      }
    }

    @Override
    public void close() {
      mPool.clear();
    }
  }
}
