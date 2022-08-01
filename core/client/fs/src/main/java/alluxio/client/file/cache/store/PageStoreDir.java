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
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.store.QuotaManagedPageStoreDir.PageReservation;
import alluxio.conf.AlluxioConfiguration;
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
public interface PageStoreDir {
  Logger LOG = LoggerFactory.getLogger(RocksPageStore.class);

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
            (LocalPageStoreOptions) pageStoreOptions,
            PageStore.create(pageStoreOptions),
            CacheEvictor.create(conf)
        );
      case ROCKS:
        return new RocksPageStoreDir(
            pageStoreOptions,
            PageStore.create(pageStoreOptions),
            CacheEvictor.create(conf)
        );
      case MEM:
        return new MemoryPageStoreDir(
            pageStoreOptions,
            (MemoryPageStore) PageStore.create(pageStoreOptions),
            CacheEvictor.create(conf)
        );
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
   * @param pageReservation
   * @return if the page added successfully
   */
  boolean putPage(PageReservation pageReservation);

  /**
   * @param fileId
   * @return if the fileId added successfully
   */
  boolean putTempFile(String fileId);

  /**
   * @param pageInfo
   * @return if the bytes requested could be reserved
   */
  Optional<PageReservation> reserve(PageInfo pageInfo,
                                    QuotaManagedPageStoreDir.ReservationOptions options);

  /**
   * @param bytes
   * @return the bytes used after release
   */
  long deletePage(PageInfo bytes);

  /**
   * Release the pre-reserved space.
   * @param reserved
   * @return the bytes used after the release
   */
  long release(PageReservation reserved);

  /**
   * @param fileId
   * @return true if the file is contained, false otherwise
   */
  boolean hasFile(String fileId);

  /**
   * @return the evictor of this dir
   */
  CacheEvictor getEvictor();

  /**
   * Close the page store dir.
   */
  void close();
}
