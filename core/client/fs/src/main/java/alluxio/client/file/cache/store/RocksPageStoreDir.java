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

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.master.metastore.rocks.RocksUtils;
import alluxio.resource.CloseableIterator;

import com.google.common.collect.Streams;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represent the dir and file level metadata of a rocksdb page store.
 */
public class RocksPageStoreDir extends QuotaManagedPageStoreDir {

  private final PageStoreOptions mPageStoreOptions;

  private RocksPageStore mPageStore;

  /**
   * Constructor of RocksPageStoreDir.
   * @param pageStoreOptions
   * @param pageStore
   * @param cacheEvictor
   */
  public RocksPageStoreDir(PageStoreOptions pageStoreOptions,
                           PageStore pageStore,
                           CacheEvictor cacheEvictor) {
    super(pageStoreOptions.getRootDir(), pageStoreOptions.getCacheSize(), cacheEvictor);
    checkState(pageStore instanceof RocksPageStore);
    mPageStore = (RocksPageStore) pageStore;
    mPageStoreOptions = pageStoreOptions;
  }

  @Override
  public PageStore getPageStore() {
    return mPageStore;
  }

  @Override
  public void reset() throws IOException {
    mPageStore.close();
    PageStoreDir.clear(getRootPath());
    // when cache is large, e.g. millions of pages, initialize may take a while on deletion
    mPageStore = (RocksPageStore) PageStore.create(mPageStoreOptions);
  }

  @Override
  public void scanPages(Consumer<Optional<PageInfo>> pageInfoConsumer) {
    try (CloseableIterator<Optional<PageInfo>> pageIterator =
        RocksUtils.createCloseableIterator(mPageStore.createNewInterator(), this::parsePageInfo)) {
      Streams.stream(pageIterator).forEach(pageInfoConsumer);
    }
  }

  private Optional<PageInfo> parsePageInfo(RocksIterator iter) {
    PageId id = RocksPageStore.getPageIdFromKey(iter.key());
    long size = iter.value().length;
    if (id != null) {
      return Optional.of(new PageInfo(id, size, this));
    }
    return Optional.empty();
  }
}
