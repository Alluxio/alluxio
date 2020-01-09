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
public class LocalCacheManager implements CacheManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheManager.class);

  private final CacheEvictor mEvictor = CacheEvictor.create();
  private final PageStore mPageStore = PageStore.create();
  private final MetaStore mMetaStore = new MetaStore();

  public LocalCacheManager() {
  }

  @Override
  public int put(long fileId, long pageId, byte[] page) throws IOException {
    mMetaStore.addPage(pageId);
    mPageStore.put(fileId, pageId, page);
    return 0;
  }

  @Override
  public ReadableByteChannel get(long fileId, long pageId) throws IOException {
    if (!mMetaStore.hasPage(pageId)) {

    }
    mEvictor.updateOnGet(pageId);
    return null;
  }

  @Override
  public boolean delete(long fileId, long pageId) throws IOException {
    mMetaStore.removePage(pageId);
    mPageStore.delete(fileId, pageId);
    return false;
  }

}
