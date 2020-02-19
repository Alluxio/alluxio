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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import javax.annotation.Nullable;

/**
 * Interface for managing cached pages.
 */
public interface CacheManager extends AutoCloseable  {
  /**
   * @param conf the Alluxio configuration
   * @return an instance of {@link CacheManager}
   */
  static CacheManager create(AlluxioConfiguration conf) throws IOException {
    // TODO(feng): make cache manager type configurable when we introduce more implementations.
    return LocalCacheManager.create(conf);
  }

  /**
   * Puts a page into the cache manager. This method is best effort. It is possible that this put
   * operation returns without page written.
   *
   * @param pageId page identifier
   * @param page page data
   * @return true if the put was successful, false otherwise
   */
  boolean put(PageId pageId, byte[] page);

  /**
   * Wraps the page in a channel or null if the queried page is not found in the cache or otherwise
   * unable to be read from the cache.
   *
   * @param pageId page identifier
   * @return a channel to read the page
   */
  @Nullable
  ReadableByteChannel get(PageId pageId);

  /**
   * Wraps a part of the page in a channel or null if the queried page is not found in the cache or
   * otherwise unable to be read from the cache.
   *
   * @param pageId page identifier
   * @param pageOffset offset into the page
   * @return a channel to read the page
   */
  @Nullable
  ReadableByteChannel get(PageId pageId, int pageOffset);

  /**
   * Deletes a page from the cache.
   *
   * @param pageId page identifier
   * @return true if the page is successfully deleted, false otherwise
   */
  boolean delete(PageId pageId);
}
