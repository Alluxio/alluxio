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

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.PageNotFoundException;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import javax.annotation.Nullable;

/**
 * Interface for managing cached pages.
 */
public interface CacheManager {
  /**
   * @param conf the Alluxio configuration
   * @return an instance of {@link CacheManager}
   */
  static CacheManager create(AlluxioConfiguration conf) {
    return new LocalCacheManager(conf);
  }

  /**
   * Writes a new page from a source channel with best effort. It is possible that this put
   * operation returns without page written due to transient behavior not due to failures writing
   * to disks.
   *
   * @param pageId page identifier
   * @param page page data
   * @throws IOException if error happens when writing the page to disk
   * @return true on a successful put or false due to transient
   */
  boolean put(PageId pageId, byte[] page) throws IOException;

  /**
   * Wraps the page in a channel or null if the queried page is not found in the cache.
   *
   * @param pageId page identifier
   * @return a channel to read the page
   * @throws IOException if error happens when reading the page
   */
  @Nullable
  ReadableByteChannel get(PageId pageId) throws IOException;

  /**
   * Wraps a part of the page in a channel or null if the queried page is not found
   * in the cache.
   *
   * @param pageId page identifier
   * @param pageOffset offset into the page
   * @return a channel to read the page
   * @throws IOException if error happens when reading the page
   */
  @Nullable
  ReadableByteChannel get(PageId pageId, int pageOffset)
      throws IOException;

  /**
   * Deletes a page from the cache.
   *
   * @param pageId page identifier
   * @throws PageNotFoundException if page is not found in the store
   */
  void delete(PageId pageId) throws IOException, PageNotFoundException;
}
