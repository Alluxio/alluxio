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

import alluxio.client.file.FileSystemContext;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Interface for managing cached pages.
 */
public interface CacheManager {
  /**
   * @param fsContext filesystem context
   * @return an instance of {@link CacheManager}
   */
  static CacheManager create(FileSystemContext fsContext) {
    return new LocalCacheManager(fsContext);
  }

  /**
   * Writes a new page from a source channel with best effort.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @param src source channel to read this new page
   * @throws IOException
   * @return the number of bytes written
   */
  int put(long fileId, long pageIndex, ReadableByteChannel src) throws IOException;

  /**
   * Reads a page to the destination channel.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @param dst destination channel to read this new page
   * @return the number of bytes read
   */
  int get(long fileId, long pageIndex, WritableByteChannel dst) throws IOException;

  /**
   * Reads a part of a page to the destination channel.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @param pageOffset offset into the page
   * @param length length to read
   * @param dst destination channel to read this new page
   * @return the number of bytes read
   */
  int get(long fileId, long pageIndex, int pageOffset, int length, WritableByteChannel dst)
      throws IOException;

  /**
   * Deletes a page from the cache.
   *
   * @param fileId file identifier
   * @param pageIndex index of the page within the file
   * @return if the page was deleted
   * @throws IOException
   */
  boolean delete(long fileId, long pageIndex) throws IOException;
}
