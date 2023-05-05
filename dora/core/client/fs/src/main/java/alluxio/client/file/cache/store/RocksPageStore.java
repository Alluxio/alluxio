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

import alluxio.Constants;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.file.ReadTargetBuffer;
import alluxio.proto.client.Cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A page store implementation which utilizes rocksDB to persist the data. This implementation
 * will not be included to client jar by default to reduce client jar size.
 */
@NotThreadSafe
public class RocksPageStore implements PageStore {
  private static final Logger LOG = LoggerFactory.getLogger(RocksPageStore.class);
  private static final String PAGE_COLUMN = "PAGE";
  private static final byte[] CONF_KEY = "CONF".getBytes();
  private static final int DEFAULT_COLUMN_INDEX = 0;
  private static final int PAGE_COLUMN_INDEX = 1;

  private static final long WRITE_BUFFER_SIZE = 64 * Constants.MB;
  private final long mCapacity;
  private final RocksDB mDb;
  private final ColumnFamilyHandle mDefaultColumnHandle;
  private final ColumnFamilyHandle mPageColumnHandle;
  private final DBOptions mRocksOptions;

  private final WriteOptions mWriteOptions = new WriteOptions();

  /**
   * @param pageStoreOptions options for the rocks page store
   * @return a new instance of {@link PageStore} backed by RocksDB
   * @throws IOException if I/O error happens
   */
  public static RocksPageStore open(PageStoreOptions pageStoreOptions) {
    RocksDB.loadLibrary();
    // The RocksObject will be closed together with the RocksPageStore
    DBOptions rocksOptions = createDbOptions();
    RocksDB db = null;
    List<ColumnFamilyHandle> columnHandles = new ArrayList<>();
    try {
      db = openDB(pageStoreOptions, rocksOptions, columnHandles);
    } catch (RocksDBException e) {
      try {
        //clear the root dir and retry
        PageStoreDir.clear(pageStoreOptions.getRootDir());
        rocksOptions = createDbOptions();
        columnHandles = new ArrayList<>();
        db = openDB(pageStoreOptions, rocksOptions, columnHandles);
      } catch (IOException | RocksDBException ex) {
        throw new RuntimeException("Couldn't open rocksDB database", e);
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Couldn't open rocksDB database", e);
    }
    return new RocksPageStore(pageStoreOptions, rocksOptions, db,
        columnHandles.get(DEFAULT_COLUMN_INDEX), columnHandles.get(PAGE_COLUMN_INDEX));
  }

  private static DBOptions createDbOptions() {
    DBOptions rocksOptions = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    return rocksOptions;
  }

  private static RocksDB openDB(PageStoreOptions pageStoreOptions,
      DBOptions rocksOptions, List<ColumnFamilyHandle> columnHandles)
      throws RocksDBException, InvalidProtocolBufferException {
    List<ColumnFamilyDescriptor> columnDescriptors = ImmutableList.of(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor(PAGE_COLUMN.getBytes(),
            new ColumnFamilyOptions()
                .setWriteBufferSize(WRITE_BUFFER_SIZE)
                .setCompressionType(CompressionType.NO_COMPRESSION))
    );
    RocksDB db =
        RocksDB.open(rocksOptions, pageStoreOptions.getRootDir().toString(), columnDescriptors,
            columnHandles);
    byte[] confData = db.get(columnHandles.get(DEFAULT_COLUMN_INDEX), CONF_KEY);
    Cache.PRocksPageStoreOptions pOptions =
        toProto(pageStoreOptions.getPageSize(), pageStoreOptions.getCacheSize(),
            pageStoreOptions.getAlluxioVersion());
    if (confData != null) {
      Cache.PRocksPageStoreOptions persistedOptions =
          Cache.PRocksPageStoreOptions.parseFrom(confData);
      if (!persistedOptions.equals(pOptions)) {
        db.close();
        rocksOptions.close();
        throw new RocksDBException("Inconsistent configuration for RocksPageStore");
      }
    }
    db.put(columnHandles.get(DEFAULT_COLUMN_INDEX), CONF_KEY, pOptions.toByteArray());
    return db;
  }

  private static Cache.PRocksPageStoreOptions toProto(long pageSize, long cacheSize,
      String alluxioVersion) {
    return Cache.PRocksPageStoreOptions.newBuilder()
        .setCommonOptions(Cache.PPageStoreCommonOptions.newBuilder()
            .setPageSize(pageSize)
            .setCacheSize(cacheSize)
            .setAlluxioVersion(alluxioVersion)
        )
        .build();
  }

  /**
   * Creates a new instance of {@link PageStore} backed by RocksDB.
   *
   * @param pageStoreOptions options for the rocks page store
   * @param rocksOptions rocksdb options
   * @param rocksDB RocksDB instance
   * @param defaultColumnHandle default column for storing configurations
   * @param pageColumnHandle page column for staring page content
   */
  private RocksPageStore(PageStoreOptions pageStoreOptions,
      DBOptions rocksOptions,
      RocksDB rocksDB,
      ColumnFamilyHandle defaultColumnHandle,
      ColumnFamilyHandle pageColumnHandle) {
    mCapacity =
        (long) (pageStoreOptions.getCacheSize() / (1 + pageStoreOptions.getOverheadRatio()));
    mRocksOptions = rocksOptions;
    mDb = rocksDB;
    mDefaultColumnHandle = defaultColumnHandle;
    mPageColumnHandle = pageColumnHandle;
  }

  @Override
  public void put(PageId pageId, ByteBuffer page, boolean isTemporary) throws IOException {
    try {
      //TODO(beinan): support temp page for rocksdb page store
      ByteBuffer key = getKeyFromPageId(pageId, page.isDirect());
      if (page.isDirect()) {
        mDb.put(mPageColumnHandle, mWriteOptions, key, page);
      } else {
        mDb.put(mPageColumnHandle, mWriteOptions, key.array(), page.array());
      }
    } catch (RocksDBException e) {
      throw new IOException("Failed to store page", e);
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, ReadTargetBuffer target,
      boolean isTemporary) throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    try {
      byte[] key = getKeyFromPageId(pageId, false).array();
      byte[] page = mDb.get(mPageColumnHandle, key);
      if (page == null) {
        throw new PageNotFoundException(new String(key));
      }
      Preconditions.checkArgument(pageOffset <= page.length,
          "page offset %s exceeded page size %s", pageOffset, page.length);
      int bytesLeft =
          Math.min(page.length - pageOffset, Math.min((int) target.remaining(), bytesToRead));
      System.arraycopy(page, pageOffset, target.byteArray(), (int) target.offset(), bytesLeft);
      return bytesLeft;
    } catch (RocksDBException e) {
      throw new IOException("Failed to retrieve page", e);
    }
  }

  @Override
  public void delete(PageId pageId) throws PageNotFoundException {
    try {
      byte[] key = getKeyFromPageId(pageId, false).array();
      mDb.delete(mPageColumnHandle, key);
    } catch (RocksDBException e) {
      throw new PageNotFoundException("Failed to remove page", e);
    }
  }

  @Override
  public void close() {
    LOG.info("Closing RocksPageStore and recycling all RocksDB JNI objects");
    mDb.close();
    mRocksOptions.close();
    mDefaultColumnHandle.close();
    mPageColumnHandle.close();
    LOG.info("RocksPageStore closed");
  }

  static ByteBuffer getKeyFromPageId(PageId pageId, boolean isDirect) {
    byte[] fileId = pageId.getFileId().getBytes();
    ByteBuffer buf;
    if (isDirect) {
      buf = ByteBuffer.allocateDirect(Long.BYTES + fileId.length);
    } else {
      buf = ByteBuffer.allocate(Long.BYTES + fileId.length);
    }
    buf.putLong(pageId.getPageIndex());
    buf.put(fileId);
    return buf;
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
    return mDb.newIterator(mPageColumnHandle);
  }
}
