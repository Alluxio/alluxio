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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link MemoryPageStore} is an implementation of {@link PageStore} which
 * stores all pages in memory.
 */
@NotThreadSafe
public class MemoryPageStore implements PageStore {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryPageStore.class);

  private ConcurrentHashMap<PageId, byte[]> mPageStoreMap = new ConcurrentHashMap<>();

  @Override
  public void put(PageId pageId, byte[] page, boolean isTemporary) throws IOException {
    //TODO(beinan): support temp page for memory page store
    PageId pageKey = getKeyFromPageId(pageId);
    try {
      byte[] pageCopy = new byte[page.length];
      System.arraycopy(page, 0, pageCopy, 0, page.length);
      mPageStoreMap.put(pageKey, pageCopy);
    } catch (Exception e) {
      throw new IOException("Failed to put cached data in memory for page " + pageId);
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int bufferOffset,
      boolean isTemporary) throws IOException, PageNotFoundException {
    Preconditions.checkArgument(buffer != null, "buffer is null");
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    Preconditions.checkArgument(buffer.length >= bufferOffset,
        "page offset %s should be " + "less or equal than buffer length %s", bufferOffset,
        buffer.length);
    PageId pageKey = getKeyFromPageId(pageId);
    if (!mPageStoreMap.containsKey(pageKey)) {
      throw new PageNotFoundException(pageId.getFileId() + "_" + pageId.getPageIndex());
    }
    byte[] page = mPageStoreMap.get(pageKey);
    Preconditions.checkArgument(pageOffset <= page.length, "page offset %s exceeded page size %s",
        pageOffset, page.length);
    int bytesLeft = (int) Math.min(page.length - pageOffset, buffer.length - bufferOffset);
    bytesLeft = Math.min(bytesLeft, bytesToRead);
    System.arraycopy(page, pageOffset, buffer, bufferOffset, bytesLeft);
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

  /**
   *
   */
  public void reset() {
    mPageStoreMap.clear();
  }
}
