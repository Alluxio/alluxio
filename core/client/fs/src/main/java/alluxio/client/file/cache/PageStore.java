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
import alluxio.util.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
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
   * Creates a {@link PageStore}. When init is false, restore from previous state; clean up the
   * cache dir otherwise.
   *
   * @param options the options to instantiate the page store
   * @param init whether to init the cache dir
   * @return a PageStore instance
   * @throws IOException if I/O error happens
   */
  static PageStore create(PageStoreOptions options, boolean init) throws IOException {
    LOG.info("Creating PageStore with option={}, init={}", options.toString(), init);
    if (init) {
      initialize(options);
    }
    final PageStore pageStore;
    switch (options.getType()) {
      case LOCAL:
        pageStore = new LocalPageStore(options.toOptions());
        break;
      case ROCKS:
        pageStore = RocksPageStore.create(options.toOptions());
        break;
      default:
        throw new IllegalArgumentException(
            "Incompatible PageStore " + options.getType() + " specified");
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
    PageStoreType storeType = options.getType();
    Path storePath = getStorePath(storeType, rootPath);
    Files.createDirectories(storePath);
    LOG.debug("Clean cache directory {}", rootPath);
    try (Stream<Path> stream = Files.list(Paths.get(rootPath))) {
      stream.forEach(path -> {
        try {
          FileUtils.deletePathRecursively(path.toString());
        } catch (IOException e) {
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
   */
  void put(PageId pageId, byte[] page) throws IOException;

  /**
   * Wraps a page from the store as a channel to read.
   *
   * @param pageId page identifier
   * @return the channel to read this page
   * @throws IOException when the store fails to read this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  default ReadableByteChannel get(PageId pageId) throws IOException, PageNotFoundException {
    return get(pageId, 0);
  }

  /**
   * Gets part of a page from the store to the destination channel.
   *
   * @param pageId page identifier
   * @param pageOffset offset within page
   * @return the number of bytes read
   * @throws IOException when the store fails to read this page
   * @throws PageNotFoundException when the page isn't found in the store
   * @throws IllegalArgumentException when the page offset exceeds the page size
   */
  ReadableByteChannel get(PageId pageId, int pageOffset) throws IOException, PageNotFoundException;

  /**
   * Deletes a page from the store.
   *
   * @param pageId page identifier
   * @param pageSize page size in bytes
   * @throws IOException when the store fails to delete this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  void delete(PageId pageId, long pageSize) throws IOException, PageNotFoundException;

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
}
