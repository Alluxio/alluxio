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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * A simple abstration on the storage to put, get and delete pages. The implementation of this class
 * does not need to provide thread-safety.
 */
public interface PageStore {

  /**
   * This enum represents the different page store implementations that can be instantiated.
   */
  enum StoreType {
    /**
     * A simple store with pages on the local filesystem
     */
    LOCAL,
    /**
     * A store that utilizes RocksDB to store and retrieve pages.
     */
    ROCKS,
  }

  /**
   * Creates a new {@link PageStore}.
   *
   * @param type the type of page store to create
   * @param dir the root storage directory
   * @return a PageStore instance
   */
  static PageStore create(StoreType type, String dir) {
    switch (type) {
      case LOCAL:
        return new LocalPageStore(dir);
      case ROCKS:
        throw new RuntimeException("rocksdb local store not yet supported");
      default:
        throw new RuntimeException("Incompatible PageStore " + type + " specified");
    }
  }

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @param src source channel to read this new page
   * @throws IOException
   * @return the number of bytes written
   */
  int put(long fileId, long pageIndex, ReadableByteChannel src) throws IOException;

  /**
   * Gets a page from the store to the destination channel.
   *
   * @param fileId file indentifier
   * @param pageIndex index of page within the file
   * @param dst destination channel to read this new page
   * @return the number of bytes read
   * @throws IOException
   */
  int get(long fileId, long pageIndex, WritableByteChannel dst) throws IOException;

  /**
   * Deletes a page from the store.
   *
   * @param fileId file identifier
   * @param pageIndex index of page within the file
   * @throws IOException
   */
  void delete(long fileId, long pageIndex) throws IOException;
}
