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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * A simple abstration on the storage to put, get and delete pages. The implementation of this class
 * does not need to provide thread-safety.
 */
public interface PageStore {

  /**
   * @return a PageStore instance
   */
  static PageStore create() {
    // return corresponding DataStore impl
    return null;
  }

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param pageId page ID
   * @param src source channel to read this new page
   * @throws IOException
   * @return the number of bytes written
   */
  int put(long pageId, ReadableByteChannel src) throws IOException;

  /**
   * Gets a page from the store to the destination channel.
   *
   * @param pageId page ID
   * @param dst destination channel to read this new page
   * @return the number of bytes read
   * @throws IOException
   */
  int get(long pageId, WritableByteChannel dst) throws IOException;

  /**
   * Deletes a page from the store.
   *
   * @param pageId page ID
   * @return if the page was deleted
   * @throws IOException
   */
  boolean delete(long pageId) throws IOException;
}
