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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class to manage cached pages. This class will
 * 1. Ensure thread-safety
 * 2. Bookkeep Cache Replacement Alg
 *
 */
@ThreadSafe
public class LocalCacheManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheManager.class);

  private final CacheEvictor mEvictor = CacheEvictor.create();
  private final PageStore mPageStore = PageStore.create();
  private final MetaStore mMetaStore = new MetaStore();

  public LocalCacheManager() {
  }

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param pageId page ID
   * @param src source channel to read this new page
   * @throws IOException
   * @return the number of bytes written
   */
  int put(long pageId, ReadableByteChannel src) throws IOException {
    mMetaStore.addPage(pageId);
//    mPageStore.put(pageId, src);
    return 0;
  }

  /**
   * Gets a page from the store to the destination channel.
   *
   * @param pageId page ID
   * @param dst destination channel to read this new page
   * @return the number of bytes read
   * @throws IOException
   */
  int get(long pageId, WritableByteChannel dst) throws IOException {
    int ret = 0;
    if (!mMetaStore.hasPage(pageId)) {

    }
    mEvictor.updateOnGet(pageId);
    return 0;
  }

  /**
   * Deletes a page from the store.
   *
   * @param pageId page ID
   * @return if the page was deleted
   * @throws IOException
   */
  boolean delete(long pageId) throws IOException {
    mMetaStore.removePage(pageId);
//    mPageStore.delete(pageId);
    return false;
  }

}
