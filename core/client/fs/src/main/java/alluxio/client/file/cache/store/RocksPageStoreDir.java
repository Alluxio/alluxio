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

import com.google.common.collect.Streams;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nullable;

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
  public void scanPages(Consumer<Optional<PageInfo>> pageInfoConsumer) throws IOException {
    RocksIterator iter = mPageStore.createNewInterator();
    iter.seekToFirst();
    Streams.stream(new PageIterator(iter, this)).onClose(iter::close).forEach(pageInfoConsumer);
  }

  private class PageIterator implements Iterator<Optional<PageInfo>> {
    //TODO(Beinan): Using a raw RocksIterator (and many other RocksObjects) is very dangerous,
    // see github PRs #14964 and #14856
    // Basically they need to be babysitted with RocksUtils.createCloseableIterator.
    private final RocksIterator mIter;
    private final RocksPageStoreDir mRocksPageStoreDir;
    private Optional<PageInfo> mValue = Optional.empty();

    PageIterator(RocksIterator iter,
                 RocksPageStoreDir rocksPageStoreDir) {
      mIter = iter;
      mRocksPageStoreDir = rocksPageStoreDir;
    }

    @Override
    public boolean hasNext() {
      return ensureValue().isPresent();
    }

    @Override
    public Optional<PageInfo> next() {
      Optional<PageInfo> value = ensureValue();
      value.orElseThrow(NoSuchElementException::new);
      mIter.next();
      mValue = Optional.empty();
      return value;
    }

    @Nullable
    private Optional<PageInfo> ensureValue() {
      if (!mValue.isPresent()) {
        for (; mIter.isValid(); mIter.next()) {
          PageId id = RocksPageStore.getPageIdFromKey(mIter.key());
          long size = mIter.value().length;
          if (id != null) {
            mValue = Optional.of(new PageInfo(id, size, mRocksPageStoreDir));
            break;
          }
        }
      }
      return mValue;
    }
  }
}
