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

import static com.google.common.base.Preconditions.checkState;

import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;

import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Represents the dir and file level meta data of the MemPageStore.
 */
public class MemoryPageStoreDir implements PageStoreDir {

  private final long mCapacity;
  private final Path mRootPath;
  private MemoryPageStore mPageStore;

  /**
   * Constructor of MemPageStoreDir.
   * @param pageStoreOptions
   * @param pageStore
   */
  public MemoryPageStoreDir(PageStoreOptions pageStoreOptions,
                            PageStore pageStore) {
    checkState(pageStore instanceof RocksPageStore);
    mPageStore = (MemoryPageStore) pageStore;
    mCapacity = pageStoreOptions.getCacheSize();
    mRootPath = pageStoreOptions.getRootDir();
  }

  @Override
  public Path getRootPath() {
    return mRootPath;
  }

  @Override
  public PageStore getPageStore() {
    return mPageStore;
  }

  @Override
  public long getCapacity() {
    return mCapacity;
  }

  @Override
  public void resetPageStore() {
  }

  @Override
  public void restorePages(Consumer<PageInfo> pageInfoConsumer) {
    //do nothing
  }

  @Override
  public long getCachedBytes() {
    return 0;
  }
}
