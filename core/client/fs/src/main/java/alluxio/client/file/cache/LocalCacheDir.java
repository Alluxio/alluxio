package alluxio.client.file.cache;

import alluxio.client.file.cache.store.PageStoreOptions;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Directory of local cache
 */
public class LocalCacheDir {

  private final Path mRootPath;
  private final long mCapacity;

  private PageStore mPageStore;

  /**
   * Constructor for LocalCacheDir.
   * @param rootPath
   * @param pageStore
   * @param capacity
   */
  public LocalCacheDir(Path rootPath, PageStore pageStore, long capacity) {
    mRootPath = rootPath;
    mPageStore = pageStore;
    mCapacity = capacity;
    //TODO(beinan): add medium type e.g. MEM, SSD or HDD
  }

  /**
   * @return root path
   */
  public Path getPath() {
    return mRootPath;
  }

  /**
   * Getter for pageStore.
   * @return pageStore
   */
  public PageStore getPageStore() {
    return mPageStore;
  }

  /**
   * Getter for capacity.
   * @return capacity
   */
  public long getCapacity() {
    return mCapacity;
  }

  /**
   * Reset page store
   * @param options PageStoreOptions
   */
  public void resetPageStore(PageStoreOptions options) {
    try {
      mPageStore.close();
      // when cache is large, e.g. millions of pages, initialize may take a while on deletion
      mPageStore = PageStore.create(options);
    } catch (Exception e) {
      throw new RuntimeException("Reset page store failed for dir " + mRootPath.toString(), e);
    }
  }
}
