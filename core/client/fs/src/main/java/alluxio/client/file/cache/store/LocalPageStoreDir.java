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

import static alluxio.client.file.cache.store.PageStoreDir.getFileBucket;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 *
 */
public class LocalPageStoreDir extends QuotaManagedPageStoreDir {
  private static final Logger LOG = LoggerFactory.getLogger(LocalPageStoreDir.class);

  private final LocalPageStoreOptions mPageStoreOptions;
  private final int mFileBuckets;
  private final Pattern mPagePattern;

  private PageStore mPageStore;

  /**
   * Constructor for LocalCacheDir.
   * @param pageStoreOptions
   * @param pageStore
   * @param evictor
   */
  public LocalPageStoreDir(LocalPageStoreOptions pageStoreOptions,
                           PageStore pageStore,
                           CacheEvictor evictor) {
    super(pageStoreOptions.getRootDir(),
        (long) (pageStoreOptions.getCacheSize() / (1 + pageStoreOptions.getOverheadRatio())),
        evictor);
    mPageStoreOptions = pageStoreOptions;
    mPageStore = pageStore;
    mFileBuckets = pageStoreOptions.getFileBuckets();
    // pattern encoding root_path/page_size(ulong)/bucket(uint)/file_id(str)/page_idx(ulong)/
    mPagePattern = Pattern.compile(
        String.format("%s/%d/(\\d+)/([^/]+)/(\\d+)",
            Pattern.quote(pageStoreOptions.getRootDir().toString()),
            pageStoreOptions.getPageSize()));
  }

  /**
   * Getter for pageStore.
   * @return pageStore
   */
  @Override
  public PageStore getPageStore() {
    return mPageStore;
  }

  /**
   * Reset page store.
   */
  @Override
  public void resetPageStore() {
    try {
      mPageStore.close();
      // when cache is large, e.g. millions of pages, initialize may take a while on deletion
      mPageStore = PageStore.create(mPageStoreOptions);
    } catch (Exception e) {
      throw new RuntimeException("Reset page store failed for dir " + getRootPath().toString(), e);
    }
  }

  /**
   * Gets a stream of all pages from the page store. This stream needs to be closed as it may
   * open IO resources.
   *
   * @throws IOException if any error occurs
   */
  @Override
  public void restorePages(Consumer<PageInfo> pageInfoConsumer) throws IOException {
    Files.walk(getRootPath()).filter(Files::isRegularFile).map(this::getPageInfo)
        .forEach(pageInfoConsumer);
  }

  /**
   * @param path path of a file
   * @return the corresponding page info for the file otherwise null
   */
  @Nullable
  private PageInfo getPageInfo(Path path) {
    PageId pageId = getPageId(path);
    long pageSize;
    if (pageId == null) {
      LOG.error("Unrecognized page file" + path);
      return null;
    }
    try {
      pageSize = Files.size(path);
    } catch (IOException e) {
      LOG.error("Failed to get file size for " + path, e);
      return null;
    }
    return new PageInfo(pageId, pageSize, this);
  }

  /**
   * @param path path of a file
   * @return the corresponding page id, or null if the file name does not match the pattern
   */
  @Nullable
  private PageId getPageId(Path path) {
    Matcher matcher = mPagePattern.matcher(path.toString());
    if (!matcher.matches()) {
      return null;
    }
    try {
      String fileBucket = Preconditions.checkNotNull(matcher.group(1));
      String fileId = Preconditions.checkNotNull(matcher.group(2));
      if (!fileBucket.equals(getFileBucket(mFileBuckets, fileId))) {
        return null;
      }
      String fileName = Preconditions.checkNotNull(matcher.group(3));
      long pageIndex = Long.parseLong(fileName);
      return new PageId(fileId, pageIndex);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
