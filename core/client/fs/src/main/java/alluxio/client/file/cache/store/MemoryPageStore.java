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
import alluxio.exception.status.ResourceExhaustedException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * The {@link MemoryPageStore} is an implementation of {@link PageStore} which
 * stores all pages in memory.
 */
@NotThreadSafe
public class MemoryPageStore implements PageStore {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryPageStore.class);
  private final long mPageSize;
  private final long mCapacity;

  private ConcurrentHashMap<PageId, byte[]> mPageStoreMap = null;

  /**
   * Creates a new instance of {@link MemoryPageStore}.
   *
   * @param options options for the buffer store
   */
  public MemoryPageStore(MemoryPageStoreOptions options) {
    mPageSize = options.getPageSize();
    mCapacity = (long) (options.getCacheSize() / (1 + options.getOverheadRatio()));
    mPageStoreMap = new ConcurrentHashMap<>();
  }

  @Override
  public void put(PageId pageId, byte[] page) throws ResourceExhaustedException, IOException {
    PageId pageKey = getKeyFromPageId(pageId);
    try {
      byte[] mPage = new byte[page.length];
      System.arraycopy(page, 0, mPage, 0, page.length);
      mPageStoreMap.put(pageKey, mPage);
    } catch (Exception e) {
      throw new IOException("Failed to put cached data in memory for page " + pageId);
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int bufferOffset)
      throws IOException, PageNotFoundException {
    Preconditions.checkArgument(buffer != null, "buffer is null");
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    Preconditions.checkArgument(buffer.length >= bufferOffset,
        "page offset %s should be " + "less or equal than buffer length %s", bufferOffset,
        buffer.length);
    PageId pageKey = getKeyFromPageId(pageId);
    if (!mPageStoreMap.containsKey(pageKey)) {
      throw new PageNotFoundException(pageId.getFileId() + "_" + pageId.getPageIndex());
    }
    byte[] mPage = mPageStoreMap.get(pageKey);
    Preconditions.checkArgument(pageOffset <= mPage.length, "page offset %s exceeded page size %s",
        pageOffset, mPage.length);
    int bytesLeft = (int) Math.min(mPage.length - pageOffset, buffer.length - bufferOffset);
    bytesLeft = Math.min(bytesLeft, bytesToRead);
    System.arraycopy(mPage, pageOffset, buffer, bufferOffset, bytesLeft);
    return bytesLeft;
  }

  @Override
  public void delete(PageId pageId) throws IOException, PageNotFoundException {
    PageId pageKey = getKeyFromPageId(pageId);
    if (!mPageStoreMap.containsKey(pageKey)) {
      throw new PageNotFoundException(pageId.getFileId() + "_" + pageId.getPageIndex());
    }
    mPageStoreMap.remove(pageKey);
    LOG.info("Remove cached page, size: {}", mPageStoreMap.size());
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
  }

  @Override
  public Stream<PageInfo> getPages() throws IOException {
    return (new ArrayList<PageInfo>(0)).stream();
  }

  @Override
  public long getCacheSize() {
    return mCapacity;
  }
}
