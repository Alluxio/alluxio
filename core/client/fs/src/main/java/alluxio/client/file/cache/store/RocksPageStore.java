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

package alluxio.client.file.cache.store;

import alluxio.client.file.cache.PageId;
import alluxio.exception.PageNotFoundException;
import alluxio.client.file.cache.PageStore;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A page store implementation which utilizes rocksDB to persist the data.
 */
@NotThreadSafe
public class RocksPageStore implements PageStore {
  private static final Logger LOG = LoggerFactory.getLogger(RocksPageStore.class);

  private final String mRoot;
  private final RocksDB mDb;
  private final AtomicInteger mSize = new AtomicInteger(0);

  /**
   * Creates a new instance of {@link PageStore} backed by RocksDB.
   *
   * @param options options for the rocks page store
   */
  public RocksPageStore(RocksPageStoreOptions options) {
    Preconditions.checkArgument(options.getMaxPageSize() > 0);
    mRoot = options.getRootDir();
    RocksDB.loadLibrary();
    try {
      Options rocksOptions = new Options();
      rocksOptions.setCreateIfMissing(true);
      rocksOptions.setWriteBufferSize(options.getWriteBufferSize());
      rocksOptions.setCompressionType(options.getCompressionType());
      mDb = RocksDB.open(rocksOptions, options.getRootDir());
    } catch (RocksDBException e) {
      throw new RuntimeException("Couldn't open rocksDB database", e);
    }
  }

  @Override
  public void put(PageId pageId, byte[] page) throws IOException {
    try {
      mDb.put(getPageKey(pageId), page);
      mSize.incrementAndGet();
    } catch (RocksDBException e) {
      throw new IOException("Failed to store page", e);
    }
  }

  @Override
  public ReadableByteChannel get(PageId pageId, int pageOffset)
      throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    try {
      byte[] page = mDb.get(getPageKey(pageId));
      if (page == null) {
        throw new PageNotFoundException(new String(getPageKey(pageId)));
      }
      Preconditions.checkArgument(pageOffset <= page.length,
          "page offset %s exceeded page size %s", pageOffset, page.length);
      ByteArrayInputStream bais = new ByteArrayInputStream(page);
      bais.skip(pageOffset);
      return Channels.newChannel(bais);
    } catch (RocksDBException e) {
      throw new IOException("Failed to retrieve page", e);
    }
  }

  @Override
  public void delete(PageId pageId) throws PageNotFoundException {
    try {
      mDb.delete(getPageKey(pageId));
      mSize.decrementAndGet();
    } catch (RocksDBException e) {
      throw new PageNotFoundException("Failed to remove page", e);
    }
  }

  @Override
  public void close() {
    mDb.close();
    try {
      FileUtils.deleteDirectory(new File(mRoot));
    } catch (IOException e) {
      LOG.warn("Failed to clean up rocksDB root directory.");
    }
  }

  private byte[] getPageKey(PageId pageId) {
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(pageId.getFileId());
    buf.putLong(pageId.getPageIndex());
    return buf.array();
  }

  @Override
  public int size() {
    return mSize.get();
  }
}
