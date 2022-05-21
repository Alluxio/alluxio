package alluxio.client.file.cache.store;

import static com.google.common.base.Preconditions.checkState;

import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.conf.AlluxioConfiguration;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Directory of page store.
 */
public interface PageStoreDir {
  /**
   * Create a list of PageStoreDir based on the configuration.
   * @param conf AlluxioConfiguration
   * @return A list of LocalCacheDir
   * @throws IOException
   */
  static List<PageStoreDir> createPageStoreDirs(AlluxioConfiguration conf)
      throws IOException {
    return PageStoreOptions.create(conf).stream().map(PageStoreDir::createPageStoreDir)
        .collect(ImmutableList.toImmutableList());
  }

  /**
   * Create an instance of PageStoreDir.
   * @param pageStoreOptions
   * @return PageStoreDir
   */
  static PageStoreDir createPageStoreDir(PageStoreOptions pageStoreOptions) {
    switch (pageStoreOptions.getType()) {
      case LOCAL:
        checkState(pageStoreOptions instanceof LocalPageStoreOptions);
        return new LocalPageStoreDir(
            (LocalPageStoreOptions) pageStoreOptions,
            PageStore.openOrCreatePageStore(pageStoreOptions));
      case ROCKS:
      case MEM:
      default:
        throw new IllegalArgumentException(String.format("Unrecognized store type %s",
            pageStoreOptions.getType().name()));
    }
  }

  /**
   * @param fileBuckets number of buckets
   * @param fileId file id
   * @return file bucket
   */
  static String getFileBucket(int fileBuckets, String fileId) {
    return Integer.toString(Math.floorMod(fileId.hashCode(), fileBuckets));
  }

  /**
   * @return root path
   */
  Path getRootPath();

  /**
   * Getter for pageStore.
   * @return pageStore
   */
  PageStore getPageStore();

  /**
   * Getter for capacity.
   * @return capacity
   */
  long getCapacity();

  /**
   * Reset page store.
   */
  void resetPageStore();

  /**
   * Traverse the pages under this dir.
   * @param pageInfoConsumer
   * @throws IOException
   */
  void restorePages(Consumer<PageInfo> pageInfoConsumer) throws IOException;

  /**
   * @return cached bytes in this directory
   */
  long getCachedBytes();
}
