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

import alluxio.client.file.cache.CacheUsage;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.quota.CacheScope;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class LocalPageStoreDir extends QuotaManagedPageStoreDir {
  private static final Logger LOG = LoggerFactory.getLogger(LocalPageStoreDir.class);

  private final PageStoreOptions mPageStoreOptions;
  private final int mFileBuckets;
  private final Pattern mPagePattern;

  private PageStore mPageStore;

  /**
   * Constructor for LocalCacheDir.
   * @param pageStoreOptions
   * @param pageStore
   * @param evictor
   */
  public LocalPageStoreDir(PageStoreOptions pageStoreOptions,
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
  public void reset() throws IOException {
    close();
    // when cache is large, e.g. millions of pages, the clear may take a while on deletion
    PageStoreDir.clear(getRootPath());
    mPageStore = PageStore.create(mPageStoreOptions);
  }

  /**
   * Gets a stream of all pages from the page store. This stream needs to be closed as it may
   * open IO resources.
   *
   * @throws IOException if any error occurs
   */
  @Override
  public void scanPages(Consumer<Optional<PageInfo>> pageInfoConsumer) throws IOException {
    Files.walk(getRootPath()).filter(Files::isRegularFile).map(this::getPageInfo)
        .forEach(pageInfoConsumer);
  }

  /**
   * @param path path of a file
   * @return the corresponding page info for the file otherwise empty
   */
  private Optional<PageInfo> getPageInfo(Path path) {
    Optional<PageId> pageId = getPageId(path);
    if (pageId.isPresent()) {
      long pageSize;
      long createdTime;
      try {
        pageSize = Files.size(path);
        FileTime creationTime = (FileTime) Files.getAttribute(path, "creationTime");
        createdTime = creationTime.toMillis();
      } catch (IOException e) {
        LOG.error("Failed to get file size for " + path, e);
        deleteUnrecognizedPage(path);
        return Optional.empty();
      }
      return Optional.of(new PageInfo(pageId.get(),
          pageSize, CacheScope.GLOBAL, this, createdTime));
    }
    deleteUnrecognizedPage(path);
    return Optional.empty();
  }

  /**
   * Deletes an unrecognized page file due to various reasons.
   * @param path the file path of a page file
   */
  private void deleteUnrecognizedPage(Path path) {
    Preconditions.checkState(path.startsWith(getRootPath()),
        String.format("%s is not inside the cache dir (%s)!", path, getRootPath()));
    try {
      Files.delete(path);
    } catch (IOException e) {
      // ignore.
    }
  }

  /**
   * @param path path of a file
   * @return the corresponding page id, or empty if the file name does not match the pattern
   */
  private Optional<PageId> getPageId(Path path) {
    Matcher matcher = mPagePattern.matcher(path.toString());
    if (!matcher.matches()) {
      // @TODO(hua) define the mPagePattern and TEMP page Pattern as static class member to save
      // CPU time and memory footprint.
      if (Pattern.matches(String.format("%s/%d/%s/([^/]+)/(\\d+)",
          Pattern.quote(mPageStoreOptions.getRootDir().toString()),
          mPageStoreOptions.getPageSize(), LocalPageStore.TEMP_DIR), path.toString())) {
        LOG.info("TEMP page file " + path + " is going to be deleted.");
      } else {
        LOG.error("Unrecognized page file " + path + "is going to be deleted.");
      }
      deleteUnrecognizedPage(path);
      return Optional.empty();
    }
    try {
      String fileBucket = Preconditions.checkNotNull(matcher.group(1));
      String fileId = Preconditions.checkNotNull(matcher.group(2));
      if (!fileBucket.equals(getFileBucket(mFileBuckets, fileId))) {
        LOG.error("Bucket number mismatch " + path);
        deleteUnrecognizedPage(path);
        return Optional.empty();
      }
      String fileName = Preconditions.checkNotNull(matcher.group(3));
      long pageIndex = Long.parseLong(fileName);
      return Optional.of(new PageId(fileId, pageIndex));
    } catch (NumberFormatException e) {
      LOG.error("Illegal numbers in path " + path);
      deleteUnrecognizedPage(path);
      return Optional.empty();
    }
  }

  @Override
  public Optional<CacheUsage> getUsage() {
    return Optional.of(new QuotaManagedPageStoreDir.Usage());
  }
}
