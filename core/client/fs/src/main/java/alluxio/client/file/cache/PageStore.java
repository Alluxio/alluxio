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
import alluxio.client.file.cache.store.MemoryPageStore;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.RocksPageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.file.ReadTargetBuffer;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A simple abstraction on the storage to put, get and delete pages. The implementation of this
 * class does not need to provide thread-safety.
 */
public interface PageStore extends AutoCloseable {
  Logger LOG = LoggerFactory.getLogger(PageStore.class);

  /**
   * Create an instance of PageStore.
   *
   * @param options the options to instantiate the page store
   * @return a PageStore instance
   */
  static PageStore create(PageStoreOptions options) {
    LOG.info("Opening PageStore with option={}", options.toString());
    final PageStore pageStore;
    switch (options.getType()) {
      case LOCAL:
        pageStore = new LocalPageStore(options);
        break;
      case ROCKS:
        pageStore = RocksPageStore.open(options);
        break;
      case MEM:
        pageStore = new MemoryPageStore((int) options.getPageSize());
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
   * Writes a new temporary page from a source channel to the store.
   *
   * @param pageId page identifier
   * @param page page data
   * @throws ResourceExhaustedException when there is not enough space found on disk
   * @throws IOException when the store fails to write this page
   */
  default void putTemporary(PageId pageId,
      byte[] page) throws ResourceExhaustedException, IOException {
    put(pageId, page, true);
  }

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param pageId page identifier
   * @param page page data
   * @throws ResourceExhaustedException when there is not enough space found on disk
   * @throws IOException when the store fails to write this page
   */
  default void put(PageId pageId,
      byte[] page) throws ResourceExhaustedException, IOException {
    put(pageId, page, false);
  }

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param pageId page identifier
   * @param page page data
   * @param isTemporary is page data temporary
   * @throws ResourceExhaustedException when there is not enough space found on disk
   * @throws IOException when the store fails to write this page
   */
  default void put(PageId pageId,
      byte[] page,
      boolean isTemporary) throws ResourceExhaustedException, IOException {
    put(pageId, ByteBuffer.wrap(page), isTemporary);
  }

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param pageId page identifier
   * @param page page data
   * @param isTemporary is page data temporary
   * @throws ResourceExhaustedException when there is not enough space found on disk
   * @throws IOException when the store fails to write this page
   */
  void put(PageId pageId,
      ByteBuffer page,
      boolean isTemporary) throws ResourceExhaustedException, IOException;

  /**
   * Gets a page from the store to the destination buffer.
   *
   * @param pageId page identifier
   * @param buffer destination buffer
   * @return the number of bytes read
   * @throws IOException when the store fails to read this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  default int get(PageId pageId, ReadTargetBuffer buffer)
      throws IOException, PageNotFoundException {
    return get(pageId, 0, (int) buffer.remaining(), buffer, false);
  }

  /**
   * Gets part of a page from the store to the destination buffer.
   *
   * @param pageId page identifier
   * @param pageOffset offset within page
   * @param bytesToRead bytes to read in this page
   * @param buffer destination buffer
   * @return the number of bytes read
   * @throws IOException when the store fails to read this page
   * @throws PageNotFoundException when the page isn't found in the store
   * @throws IllegalArgumentException when the page offset exceeds the page size
   */
  default int get(PageId pageId, int pageOffset, int bytesToRead, ReadTargetBuffer buffer)
      throws IOException, PageNotFoundException {
    return get(pageId, pageOffset, bytesToRead, buffer, false);
  }

  /**
   * Gets part of a page from the store to the destination buffer.
   *
   * @param pageId page identifier
   * @param pageOffset offset within page
   * @param bytesToRead bytes to read in this page
   * @param buffer destination buffer
   * @param isTemporary is page data temporary
   * @return the number of bytes read
   * @throws IOException when the store fails to read this page
   * @throws PageNotFoundException when the page isn't found in the store
   * @throws IllegalArgumentException when the page offset exceeds the page size
   */
  int get(PageId pageId, int pageOffset, int bytesToRead, ReadTargetBuffer buffer,
      boolean isTemporary)
      throws IOException, PageNotFoundException;

  /**
   * Deletes a temporary page from the store.
   *
   * @param pageId page identifier
   * @param isTemporary whether is to delete the temporary page or not
   * @throws IOException when the store fails to delete this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  default void delete(PageId pageId, boolean isTemporary)
      throws IOException, PageNotFoundException {
    delete(pageId);
  }

  /**
   * Deletes a page from the store.
   *
   * @param pageId page identifier
   * @throws IOException when the store fails to delete this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  default void delete(PageId pageId) throws IOException, PageNotFoundException {
    delete(pageId, false);
  }

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
  default void commit(String fileId, String newFileId) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Abort a temporary file.
   * @param fileId
   */
  default void abort(String fileId) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Metrics.
   */
  final class Metrics {
    /**
     * Number of failures when cleaning out the existing cache directory
     * to initialize a new cache.
     */
    public static final Counter CACHE_CLEAN_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_CLEAN_ERRORS.getName());
  }
}
