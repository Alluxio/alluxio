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

import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.CacheStatus;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.CacheEvictorOptions;
import alluxio.util.io.FileUtils;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Directory of page store.
 */
public interface PageStoreDir extends CacheStatus {
  Logger LOG = LoggerFactory.getLogger(PageStoreDir.class);

  /**
   * Create a list of PageStoreDir based on the configuration.
   * @param options of cache manager
   * @return A list of LocalCacheDir
   * @throws IOException
   */
  static List<PageStoreDir> createPageStoreDirs(CacheManagerOptions options)
      throws IOException {
    return options.getPageStoreOptions().stream()
        .map(pageStoreOptions -> createPageStoreDir(options.getCacheEvictorOptions(),
            pageStoreOptions))
        .collect(ImmutableList.toImmutableList());
  }

  /**
   * Create an instance of PageStoreDir.
   *
   * @param cacheEvictorOptions
   * @param pageStoreOptions
   * @return PageStoreDir
   */
  static PageStoreDir createPageStoreDir(CacheEvictorOptions cacheEvictorOptions,
                                         PageStoreOptions pageStoreOptions) {
    switch (pageStoreOptions.getType()) {
      case LOCAL:
        return new LocalPageStoreDir(
            pageStoreOptions,
            PageStore.create(pageStoreOptions),
            CacheEvictor.create(cacheEvictorOptions)
        );
      case MEM:
        return new MemoryPageStoreDir(
            pageStoreOptions,
            (MemoryPageStore) PageStore.create(pageStoreOptions),
            CacheEvictor.create(cacheEvictorOptions)
        );
      case RAWDEVICE:
        return new RawDevicePageStoreDir(pageStoreOptions,
            alluxio.cachestore.RawDeviceStore.getInstance(),
            CacheEvictor.create(cacheEvictorOptions));
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
   * Clear the dir.
   * @param rootPath
   * @throws IOException when failed to clean up the specific location
   */
  static void clear(Path rootPath) throws IOException {
    Files.createDirectories(rootPath);
    LOG.info("Cleaning cache directory {}", rootPath);
    try (Stream<Path> stream = Files.list(rootPath)) {
      stream.forEach(path -> {
        try {
          FileUtils.deletePathRecursively(path.toString());
        } catch (IOException e) {
          PageStore.Metrics.CACHE_CLEAN_ERRORS.inc();
          LOG.warn("failed to delete {} in cache directory: {}", path,
              e.toString());
        }
      });
    }
  }

  /**
   * @return root path
   */
  Path getRootPath();

  /**
   * @return pageStore
   */
  PageStore getPageStore();

  /**
   * @return capacity
   */
  long getCapacityBytes();

  /**
   * Reset page store.
   */
  void reset() throws IOException;

  /**
   * Scan the pages under this dir.
   * @param pageInfoConsumer
   * @throws IOException
   */
  void scanPages(Consumer<Optional<PageInfo>> pageInfoConsumer) throws IOException;

  /**
   * @return cached bytes in this directory
   */
  long getCachedBytes();

  /**
   * @param pageInfo
   */
  void putPage(PageInfo pageInfo);

  /**
   * @param pageInfo
   */
  void putTempPage(PageInfo pageInfo);

  /**
   * @param fileId file id
   * @return if the temp file is added successfully
   */
  boolean putTempFile(String fileId);

  /**
   * @param bytes
   * @return if the bytes requested could be reserved
   */
  boolean reserve(long bytes);

  /**
   * @param bytes
   */
  void deleteTempPage(PageInfo bytes);

  /**
   * @param bytes
   * @return the bytes used after release
   */
  long deletePage(PageInfo bytes);

  /**
   * Release the pre-reserved space.
   * @param bytes
   * @return the bytes used after the release
   */
  long release(long bytes);

  /**
   * @param fileId
   * @return true if the file is contained, false otherwise
   */
  boolean hasFile(String fileId);

  /**
   * @param fileId
   * @return true if the temp file is contained, false otherwise
   */
  boolean hasTempFile(String fileId);

  /**
   * @return the evictor of this dir
   */
  CacheEvictor getEvictor();

  /**
   * Close the page store dir.
   */
  void close();

  /**
   * Commit a temporary file.
   * @param fileId
   */
  default void commit(String fileId) throws IOException {
    commit(fileId, fileId);
  }

  /**
   * Commit a temporary file with a new file ID.
   * @param fileId
   * @param newFileId
   */
  void commit(String fileId, String newFileId) throws IOException;

  /**
   * Abort a temporary file.
   * @param fileId
   */
  void abort(String fileId) throws IOException;
}
