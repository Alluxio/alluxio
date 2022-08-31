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

package alluxio.worker.page;

import static org.junit.Assert.fail;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.store.PageReadTargetBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of cache manager that stores cached data in byte arrays in memory.
 */
class ByteArrayCacheManager implements CacheManager {
  private final Map<PageId, byte[]> mPages;

  /**
   * Metrics for test validation.
   */
  long mPagesServed = 0;
  long mPagesCached = 0;

  ByteArrayCacheManager() {
    mPages = new HashMap<>();
  }

  @Override
  public boolean put(PageId pageId, ByteBuffer page, CacheContext cacheContext) {
    byte[] data = new byte[page.remaining()];
    page.get(data);
    mPages.put(pageId, data);
    mPagesCached++;
    return true;
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, PageReadTargetBuffer target,
      CacheContext cacheContext) {
    if (!mPages.containsKey(pageId)) {
      return 0;
    }
    mPagesServed++;
    ByteBuffer data = ByteBuffer.allocate(bytesToRead);
    data.put(mPages.get(pageId), pageOffset, bytesToRead);
    data.flip();
    try {
      if (target.hasByteBuffer()) {
        target.byteBuffer().put(data);
      } else {
        target.byteChannel().write(data);
      }
    } catch (IOException e) {
      fail(e.getMessage());
    }
    return bytesToRead;
  }

  @Override
  public boolean delete(PageId pageId) {
    return mPages.remove(pageId) != null;
  }

  @Override
  public State state() {
    return State.READ_WRITE;
  }

  @Override
  public boolean append(PageId pageId, int appendAt, byte[] page, CacheContext cacheContext) {
    return false;
  }

  @Override
  public void close() throws Exception {
    // no-op
  }
}
