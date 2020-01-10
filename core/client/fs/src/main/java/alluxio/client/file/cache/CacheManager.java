package alluxio.client.file.cache;

import alluxio.client.file.cache.store.PageNotFoundException;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

interface CacheManager {

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param fileId file ID
   * @param pageId page ID
   * @param page page data
   * @return the number of bytes written
   */
  int put(long fileId, long pageId, byte[] page) throws IOException;

  /**
   * Gets a page from the store to the destination channel.
   *
   * @param fileId file ID
   * @param pageId page ID
   * @return a channel to read the page
   */
  ReadableByteChannel get(long fileId, long pageId) throws IOException;

  /**
   * Deletes a page from the store.
   *
   * @param fileId file ID
   * @param pageId page ID
   * @return if the page was deleted
   */
  boolean delete(long fileId, long pageId) throws IOException, PageNotFoundException;
}
