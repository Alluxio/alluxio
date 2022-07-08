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
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.proto.client.Cache;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
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

  private final long mCapacity;
  private final RocksDB mDb;
  private final RocksPageStoreOptions mPageStoreOptions;
  private final Options mDbOptions;

  /**
   * @param pageStoreOptions options for the rocks page store
   * @return a new instance of {@link PageStore} backed by RocksDB
   * @throws IOException if I/O error happens
   */
  public static RocksPageStore open(RocksPageStoreOptions pageStoreOptions) {
    Preconditions.checkArgument(pageStoreOptions.getMaxPageSize() > 0);
    RocksDB.loadLibrary();
    // The RocksObject will be closed together with the RocksPageStore
    Options rocksOptions = new Options()
        .setCreateIfMissing(true)
        .setWriteBufferSize(pageStoreOptions.getWriteBufferSize())
        .setCompressionType(pageStoreOptions.getCompressionType());
    RocksDB db = null;
    try {
      db = openDB(pageStoreOptions, rocksOptions);
    } catch (RocksDBException e) {
      try {
        //clear the root dir and retry
        PageStoreDir.clear(pageStoreOptions.mRootDir);
        db = openDB(pageStoreOptions, rocksOptions);
      } catch (IOException | RocksDBException ex) {
        rocksOptions.close();
        throw new RuntimeException("Couldn't open rocksDB database", e);
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Couldn't open rocksDB database", e);
    }
    return new RocksPageStore(pageStoreOptions, db, rocksOptions);
  }

  private static RocksDB openDB(RocksPageStoreOptions pageStoreOptions, Options rocksOptions)
      throws RocksDBException, InvalidProtocolBufferException {
    RocksDB db = RocksDB.open(rocksOptions, pageStoreOptions.getRootDir().toString());
    byte[] confData = db.get(CONF_KEY);
    Cache.PRocksPageStoreOptions pOptions = pageStoreOptions.toProto();
    if (confData != null) {
      Cache.PRocksPageStoreOptions persistedOptions =
          Cache.PRocksPageStoreOptions.parseFrom(confData);
      if (!persistedOptions.equals(pOptions)) {
        db.close();
        throw new RocksDBException("Inconsistent configuration for RocksPageStore");
      }
    }
    db.put(CONF_KEY, pOptions.toByteArray());
    return db;
  }

  /**
   * Creates a new instance of {@link PageStore} backed by RocksDB.
   *
   * @param pageStoreOptions options for the rocks page store
   * @param rocksDB RocksDB instance
   * @param dbOptions the RocksDB options
   */
  private RocksPageStore(RocksPageStoreOptions pageStoreOptions, RocksDB rocksDB,
      Options dbOptions) {
    mPageStoreOptions = pageStoreOptions;
    mDbOptions = dbOptions;
    mCapacity =
        (long) (pageStoreOptions.getCacheSize() / (1 + pageStoreOptions.getOverheadRatio()));
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
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int bufferOffset)
      throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    try {
      byte[] page = mDb.get(getKeyFromPageId(pageId));
      if (page == null) {
        throw new PageNotFoundException(new String(getKeyFromPageId(pageId)));
      }
      Preconditions.checkArgument(pageOffset <= page.length,
          "page offset %s exceeded page size %s", pageOffset, page.length);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(page)) {
        int bytesSkipped = (int) bais.skip(pageOffset);
        if (pageOffset != bytesSkipped) {
          throw new IOException(
              String.format("Failed to read page %s from offset %s: %s bytes skipped", pageId,
                  pageOffset, bytesSkipped));
        }
        int bytesRead = 0;
        int bytesLeft = Math.min(page.length - pageOffset, buffer.length - bufferOffset);
        bytesLeft = Math.min(bytesLeft, bytesToRead);
        while (bytesLeft >= 0) {
          int bytes = bais.read(buffer, bufferOffset + bytesRead, bytesLeft);
          if (bytes <= 0) {
            break;
          }
          bytesRead += bytes;
          bytesLeft -= bytes;
        }
        return bytesRead;
      }
    } catch (RocksDBException e) {
      throw new IOException("Failed to retrieve page", e);
    }
  }

  @Override
  public void delete(PageId pageId) throws PageNotFoundException {
    try {
      byte[] key = getKeyFromPageId(pageId);
      mDb.delete(key);
    } catch (RocksDBException e) {
      throw new PageNotFoundException("Failed to remove page", e);
    }
  }

  @Override
  public void close() {
    LOG.info("Closing RocksPageStore and recycling all RocksDB JNI objects");
    mDb.close();
    mDbOptions.close();
    LOG.info("RocksPageStore closed");
  }

  static byte[] getKeyFromPageId(PageId pageId) {
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
  static PageId getPageIdFromKey(byte[] key) {
    if (key.length < Long.BYTES) {
      return null;
    }
    ByteBuffer buf = ByteBuffer.wrap(key);
    long pageIndex = buf.getLong();
    String fileId = Charset.defaultCharset().decode(buf).toString();
    return new PageId(fileId, pageIndex);
  }

  /**
   * @return a new iterator for the rocksdb
   */
  public RocksIterator createNewInterator() {
    return mDb.newIterator();
  }
}
