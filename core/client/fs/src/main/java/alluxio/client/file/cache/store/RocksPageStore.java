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
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.proto.client.Cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A page store implementation which utilizes rocksDB to persist the data. This implementation
 * will not be included to client jar by default to reduce client jar size.
 */
@NotThreadSafe
public class RocksPageStore implements PageStore {
  private static final Logger LOG = LoggerFactory.getLogger(RocksPageStore.class);
  private static final byte[] CONF_KEY = "CONF".getBytes();
  // TODO(feng): consider making the overhead ratio configurable
  // We assume 20% overhead using Rocksdb as a page store, i.e., with 1GB space allocated, we
  // expect no more than 1024MB/(1+20%)=853MB logical data stored
  private static final double ROCKS_OVERHEAD_RATIO = 0.2;

  private final long mCapacity;
  private final RocksDB mDb;
  private final RocksPageStoreOptions mOptions;

  /**
   * @param options options for the rocks page store
   * @return a new instance of {@link PageStore} backed by RocksDB
   * @throws IOException if I/O error happens
   */
  public static RocksPageStore create(RocksPageStoreOptions options) throws IOException {
    Preconditions.checkArgument(options.getMaxPageSize() > 0);
    RocksDB.loadLibrary();
    Options rocksOptions = new Options();
    rocksOptions.setCreateIfMissing(true);
    rocksOptions.setWriteBufferSize(options.getWriteBufferSize());
    rocksOptions.setCompressionType(options.getCompressionType());
    RocksDB db = null;
    try {
      db = RocksDB.open(rocksOptions, options.getRootDir());
      byte[] confData = db.get(CONF_KEY);
      Cache.PRocksPageStoreOptions pOptions = options.toProto();
      if (confData != null) {
        Cache.PRocksPageStoreOptions persistedOptions =
            Cache.PRocksPageStoreOptions.parseFrom(confData);
        if (!persistedOptions.equals(pOptions)) {
          db.close();
          throw new IOException("Inconsistent configuration for RocksPageStore");
        }
      }
      db.put(CONF_KEY, pOptions.toByteArray());
    } catch (RocksDBException e) {
      if (db != null) {
        db.close();
      }
      throw new IOException("Couldn't open rocksDB database", e);
    }
    return new RocksPageStore(options, db);
  }

  /**
   * Creates a new instance of {@link PageStore} backed by RocksDB.
   *
   * @param options options for the rocks page store
   */
  private RocksPageStore(RocksPageStoreOptions options, RocksDB rocksDB) {
    mOptions = options;
    mCapacity = (long) (options.getCacheSize() / (1 + ROCKS_OVERHEAD_RATIO));
    mDb = rocksDB;
  }

  @Override
  public void put(PageId pageId, byte[] page) throws IOException {
    try {
      byte[] key = getKeyFromPageId(pageId);
      mDb.put(key, page);
    } catch (RocksDBException e) {
      throw new IOException("Failed to store page", e);
    }
  }

  @Override
  public ReadableByteChannel get(PageId pageId, int pageOffset)
      throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    try {
      byte[] page = mDb.get(getKeyFromPageId(pageId));
      if (page == null) {
        throw new PageNotFoundException(new String(getKeyFromPageId(pageId)));
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
  public void delete(PageId pageId, long pageSize) throws PageNotFoundException {
    try {
      byte[] key = getKeyFromPageId(pageId);
      mDb.delete(key);
    } catch (RocksDBException e) {
      throw new PageNotFoundException("Failed to remove page", e);
    }
  }

  @Override
  public void close() {
    mDb.close();
  }

  private static byte[] getKeyFromPageId(PageId pageId) {
    byte[] fileId = pageId.getFileId().getBytes();
    ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + fileId.length);
    buf.putLong(pageId.getPageIndex());
    buf.put(fileId);
    return buf.array();
  }

  /**
   * @param key key of a record
   * @return the corresponding page id, or null if the key does not match the pattern
   */
  @Nullable
  private static PageId getPageIdFromKey(byte[] key) {
    if (key.length < Long.BYTES) {
      return null;
    }
    ByteBuffer buf = ByteBuffer.wrap(key);
    long pageIndex = buf.getLong();
    String fileId = Charset.defaultCharset().decode(buf).toString();
    return new PageId(fileId, pageIndex);
  }

  @Override
  public Stream<PageInfo> getPages() {
    RocksIterator iter = mDb.newIterator();
    iter.seekToFirst();
    return Streams.stream(new PageIterator(iter)).onClose(iter::close);
  }

  @Override
  public long getCacheSize() {
    return mCapacity;
  }

  private class PageIterator implements Iterator<PageInfo> {
    private final RocksIterator mIter;
    private PageInfo mValue;

    PageIterator(RocksIterator iter) {
      mIter = iter;
    }

    @Override
    public boolean hasNext() {
      return ensureValue() != null;
    }

    @Override
    public PageInfo next() {
      PageInfo value = ensureValue();
      if (value == null) {
        throw new NoSuchElementException();
      }
      mIter.next();
      mValue = null;
      return value;
    }

    @Nullable
    private PageInfo ensureValue() {
      if (mValue == null) {
        for (; mIter.isValid(); mIter.next()) {
          PageId id = getPageIdFromKey(mIter.key());
          long size = mIter.value().length;
          if (id != null) {
            mValue = new PageInfo(id, size);
            break;
          }
        }
      }
      return mValue;
    }
  }
}
