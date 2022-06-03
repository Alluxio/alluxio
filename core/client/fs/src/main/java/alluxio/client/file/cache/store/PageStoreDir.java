package alluxio.client.file.cache.store;

import static com.google.common.base.Preconditions.checkState;

import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.conf.AlluxioConfiguration;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

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
    return PageStoreOptions.create(conf).stream().map(options -> createPageStoreDir(conf, options))
        .collect(ImmutableList.toImmutableList());
  }

  /**
   * Create an instance of PageStoreDir.
   *
   * @param conf
   * @param pageStoreOptions
   * @return PageStoreDir
   */
  static PageStoreDir createPageStoreDir(AlluxioConfiguration conf,
                                         PageStoreOptions pageStoreOptions) {
    switch (pageStoreOptions.getType()) {
      case LOCAL:
        checkState(pageStoreOptions instanceof LocalPageStoreOptions);
        return new LocalPageStoreDir(
            conf,
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

  /**
   * @param pageInfo
   * @return if the page added successfully
   */
  boolean putPageToDir(PageInfo pageInfo);

  /**
   * @param fileId
   * @return if the fileId added successfully
   */
  boolean addTempFileToDir(String fileId);


  /**
   * @param bytes
   * @return if the bytes requested could be reserved
   */
  boolean reserveSpace(int bytes);

  /**
   * @param bytes
   * @return the bytes used after release
   */
  long releaseSpace(PageInfo bytes);

  /**
   * @param fileId
   * @return true if the block is contained, false otherwise
   */
  boolean hasFile(String fileId);

  /**
   * @return the evictor of this dir
   */
  CacheEvictor getEvictor();
}
