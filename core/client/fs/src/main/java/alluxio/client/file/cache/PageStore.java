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
import alluxio.client.file.cache.store.LocalPageStoreOptions;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.client.file.cache.store.RocksPageStore;
import alluxio.client.file.cache.store.RocksPageStoreOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.util.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * A simple abstraction on the storage to put, get and delete pages. The implementation of this
 * class does not need to provide thread-safety.
 */
public interface PageStore extends AutoCloseable {
  Logger LOG = LoggerFactory.getLogger(LocalPageStore.class);

  /**
   * Creates a new {@link PageStore}.
   *
   * @param options the options to instantiate the page store
   * @return a PageStore instance
   * @throws IOException if failed to create a page store
   */
  static PageStore create(PageStoreOptions options) throws IOException {
    switch (options.getType()) {
      case LOCAL:
        return new LocalPageStore(options.toOptions());
      case ROCKS:
        return new RocksPageStore(options.toOptions());
      default:
        throw new IllegalArgumentException(
            "Incompatible PageStore " + options.getType() + " specified");
    }
  }

  /**
   * Creates a new instance of {@link PageStore} based on configuration.
   *
   * @param conf configuration
   * @return the {@link PageStore}
   */
  static PageStore create(AlluxioConfiguration conf) throws IOException {
    PageStoreOptions options;
    PageStoreType storeType = conf.getEnum(
        PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.class);
    switch (storeType) {
      case LOCAL:
        options = new LocalPageStoreOptions();
        break;
      case ROCKS:
        options = new RocksPageStoreOptions();
        break;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized store type %s",
            storeType.name()));
    }
    String rootDir = conf.get(PropertyKey.USER_CLIENT_CACHE_DIR);
    initialize(rootDir, storeType);
    Path storePath = getStorePath(storeType, rootDir);
    options.setRootDir(storePath.toString());
    options.setPageSize(conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE));
    options.setCacheSize(conf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE));
    options.setAlluxioVersion(conf.get(PropertyKey.VERSION));
    return create(options);
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
   * Initialize a page store at the configured location.
   * Data from different store type will be removed.
   *
   * @param rootPath root path of the page store
   * @param storeType the page store type
   */
  static void initialize(String rootPath, PageStoreType storeType) throws IOException {
    Path storePath = getStorePath(storeType, rootPath);
    Files.createDirectories(storePath);
    LOG.info("Clean cache directory {}", rootPath);
    try (Stream<Path> stream = Files.list(Paths.get(rootPath))) {
      stream.filter(path -> !storePath.equals(path))
          .forEach(path -> {
            try {
              FileUtils.deletePathRecursively(path.toString());
            } catch (IOException e) {
              LOG.warn("failed to delete {} in cache directory: {}", path, e.getMessage());
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
  default ReadableByteChannel get(PageId pageId) throws IOException,
      PageNotFoundException {
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
  ReadableByteChannel get(PageId pageId, int pageOffset) throws IOException,
      PageNotFoundException;

  /**
   * Deletes a page from the store.
   *
   * @param pageId page identifier
   * @throws IOException when the store fails to delete this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  void delete(PageId pageId) throws IOException, PageNotFoundException;

  /**
   * @return the number of pages stored
   */
  int size();

  /**
   * Gets all page ids.
   *
   * @return collection of ids representing all pages loaded from disk
   * @throws IOException if any error occurs
   */
  Collection<PageId> getPages() throws IOException;
}
