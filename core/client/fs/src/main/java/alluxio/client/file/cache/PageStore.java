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
import alluxio.client.file.cache.store.RocksPageStore;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.PageNotFoundException;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * A simple abstraction on the storage to put, get and delete pages. The implementation of this
 * class does not need to provide thread-safety.
 */
public interface PageStore extends AutoCloseable {

  /**
   * Creates a new {@link PageStore}.
   *
   * @param options the options to instantiate the page store
   * @return a PageStore instance
   */
  static PageStore create(PageStoreOptions options) {
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
   * Creates a new default instance of {@link PageStore}.
   * @return the default {@link PageStore}
   */
  static PageStore create() {
    return create(new LocalPageStoreOptions());
  }

  /**
   * Creates a new instance of {@link PageStore} based on configuration.
   * @param conf configuration
   * @return the {@link PageStore}
   */
  static PageStore create(AlluxioConfiguration conf) {
    return create(PageStoreOptions.create(conf));
  }

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param pageId page identifier
   * @param page page data
   */
  void put(PageId pageId, byte[] page) throws IOException;

  /**
   * Gets a page from the store to the destination channel.
   *
   * @param pageId page identifier
   * @return the number of bytes read
   * @throws IOException
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
   * @throws IOException
   * @throws PageNotFoundException when the page isn't found in the store
   * @throws IllegalArgumentException when the page offset exceeds the page size
   */
  ReadableByteChannel get(PageId pageId, int pageOffset) throws IOException,
      PageNotFoundException;

  /**
   * Deletes a page from the store.
   *
   * @param pageId page identifier
   * @throws IOException
   * @throws PageNotFoundException when the page isn't found in the store
   */
  void delete(PageId pageId) throws IOException, PageNotFoundException;

  @Override
  void close();

  /**
   * @return the number of pages stored
   */
  int size();
}
