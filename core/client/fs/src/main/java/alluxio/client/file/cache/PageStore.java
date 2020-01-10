/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.client.file.cache;

import alluxio.client.file.cache.store.LocalPageStore;
import alluxio.client.file.cache.store.LocalPageStoreOptions;
import alluxio.client.file.cache.store.PageNotFoundException;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.RocksPageStore;

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
   * Writes a new page from a source channel to the store.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @param page page data
   */
  void put(long fileId, long pageIndex, byte[] page) throws IOException;

  /**
   * Gets a page from the store to the destination channel.
   *
   * @param fileId file indentifier
   * @param pageIndex index of page within the file
   * @return the number of bytes read
   * @throws IOException
   * @throws PageNotFoundException when the page isn't found in the store
   */
  ReadableByteChannel get(long fileId, long pageIndex) throws IOException,
      PageNotFoundException;

  /**
   * Deletes a page from the store.
   *
   * @param fileId file identifier
   * @param pageIndex index of page within the file
   * @throws IOException
   * @throws PageNotFoundException when the page isn't found in the store
   */
  void delete(long fileId, long pageIndex) throws IOException, PageNotFoundException;

  @Override
  void close();

  /**
   * @return the number of pages stored
   */
  int size();
}
