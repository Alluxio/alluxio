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

package alluxio.client.file.cache;

import alluxio.client.file.cache.store.LocalPageStore;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.client.file.cache.store.RocksPageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.io.FileUtils;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * A simple abstraction on the storage to put, get and delete pages. The implementation of this
 * class does not need to provide thread-safety.
 */
public interface PageStore extends AutoCloseable {
  Logger LOG = LoggerFactory.getLogger(PageStore.class);

  /**
   * Creates a new {@link PageStore}. Previous state in the samme cache dir will be overwritten.
   *
   * @param options the options to instantiate the page store
   * @return a PageStore instance
   * @throws IOException if I/O error happens
   */
  static PageStore create(PageStoreOptions options) throws IOException {
    initialize(options);
    return open(options);
  }

  /**
   * Opens an existing {@link PageStore}.
   *
   * @param options the options to instantiate the page store
   * @return a PageStore instance
   * @throws IOException if I/O error happens
   */
  static PageStore open(PageStoreOptions options) throws IOException {
    LOG.info("Opening PageStore with option={}", options.toString());
    final PageStore pageStore;
    switch (options.getType()) {
      case LOCAL:
        pageStore = new LocalPageStore(options.toOptions());
        break;
      case ROCKS:
        pageStore = RocksPageStore.open(options.toOptions());
        break;
      default:
        throw new IllegalArgumentException(
            "Incompatible PageStore " + options.getType() + " specified");
    }
    if (options.getTimeoutDuration() > 0) {
      return new TimeBoundPageStore(pageStore, options);
    }
    return pageStore;
  }

  /**
   * Gets store path given root directory and store type.
   *
   * @param storeType the type of the page store
   * @param rootDir the root directory path
   * @return the store directory path
   */
  static Path getStorePath(PageStoreType storeType, String rootDir) {
    return Paths.get(rootDir, storeType.name());
  }

  /**
   * Initializes a page store at the configured location.
   * Data from different store type will be removed.
   *
   * @param options initialize a new page store based on the options
   * @throws IOException when failed to clean up the specific location
   */
  static void initialize(PageStoreOptions options) throws IOException {
    String rootPath = options.getRootDir();
    Files.createDirectories(Paths.get(rootPath));
    LOG.info("Cleaning cache directory {}", rootPath);
    try (Stream<Path> stream = Files.list(Paths.get(rootPath))) {
      stream.forEach(path -> {
        try {
          FileUtils.deletePathRecursively(path.toString());
        } catch (IOException e) {
          Metrics.CACHE_CLEAN_ERRORS.inc();
          LOG.warn("failed to delete {} in cache directory: {}", path, e.toString());
        }
      });
    }
  }

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param pageId page identifier
   * @param page page data
   * @throws ResourceExhaustedException when there is not enough space found on disk
   * @throws IOException when the store fails to write this page
   */
  void put(PageId pageId, byte[] page) throws ResourceExhaustedException, IOException;

  /**
   * Gets a page from the store to the destination buffer.
   *
   * @param pageId page identifier
   * @param buffer destination buffer
   * @return the number of bytes read
   * @throws IOException when the store fails to read this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  default int get(PageId pageId, byte[] buffer) throws IOException, PageNotFoundException {
    return get(pageId, 0, buffer.length, buffer, 0);
  }

  /**
   * Gets part of a page from the store to the destination buffer.
   *
   * @param pageId page identifier
   * @param pageOffset offset within page
   * @param bytesToRead bytes to read in this page
   * @param buffer destination buffer
   * @param bufferOffset offset in buffer
   * @return the number of bytes read
   * @throws IOException when the store fails to read this page
   * @throws PageNotFoundException when the page isn't found in the store
   * @throws IllegalArgumentException when the page offset exceeds the page size
   */
  int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int bufferOffset)
      throws IOException, PageNotFoundException;

  /**
   * Deletes a page from the store.
   *
   * @param pageId page identifier
   * @throws IOException when the store fails to delete this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  void delete(PageId pageId) throws IOException, PageNotFoundException;

  /**
   * Gets a stream of all pages from the page store. This stream needs to be closed as it may
   * open IO resources.
   *
   * @return a stream of all pages from page store
   * @throws IOException if any error occurs
   */
  Stream<PageInfo> getPages() throws IOException;

  /**
   * @return an estimated cache size in bytes
   */
  long getCacheSize();

  /**
   * Metrics.
   */
  final class Metrics {
    /**
     * Number of failures when cleaning out the existing cache directory
     * to initialize a new cache.
     */
    private static final Counter CACHE_CLEAN_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_CLEAN_ERRORS.getName());
  }
}
