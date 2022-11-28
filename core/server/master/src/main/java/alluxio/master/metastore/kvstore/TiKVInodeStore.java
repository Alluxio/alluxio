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

package alluxio.master.metastore.kvstore;

import alluxio.collections.Pair;
import alluxio.kvstore.KVStoreFactory;
import alluxio.kvstore.KVStoreInterface;
import alluxio.kvstore.KVStoreMetaInterface;
import alluxio.kvstore.KVStoreMountInterface;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.metastore.KVInodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.proto.kvstore.FileCacheStatusKey;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.kvstore.KVEntryType;
import alluxio.proto.kvstore.KVStoreTable;
import alluxio.resource.CloseableIterator;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.shade.com.google.protobuf.ByteString;

import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * File store backed by RocksDB.
 */
@ThreadSafe
public class TiKVInodeStore implements KVInodeStore {
  private static final Logger LOG = LoggerFactory.getLogger(TiKVInodeStore.class);

  private final KVStoreInterface mKVStoreInterface;
  private final KVStoreMetaInterface mKVStoreMetaInterface;
  private final KVStoreMountInterface mKVStoreMountInterface;

  /**
   * Creates and initializes a rocks block store.
   *
   */
  public TiKVInodeStore() {
    mKVStoreInterface = KVStoreFactory.getKVStoreFactory();
    mKVStoreMetaInterface = mKVStoreInterface.getMetaKVStore();
    mKVStoreMountInterface = mKVStoreInterface.getMountKVStore();
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    FileEntryKey fileEntryKey = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(inode.getParentId())
        .setName(inode.getName())
        .build();
    // fill in the values
    FileEntryValue fileEntryValue = FileEntryValue.newBuilder()
        .setMID(inode.getId())
        .setMEntryType(inode.isDirectory() ? KVEntryType.DIRECTORY : KVEntryType.FILE)
        .setMEV(KVStoreUtils.convertMutableInodeToByteString(inode))
        .build();
    // TODO(yyong) figure out the format of metadata part
    // Need to check if the entry exists.
    // mKVStoreMetaInterface.createFileEntry(fileEntryKey, fileEntryValue);
    // So far only update, the caller should make sure the key not existing
    mKVStoreMetaInterface.updateFileEntry(fileEntryKey, fileEntryValue);
  }

  @Override
  public WriteBatch createWriteBatch() {
    return new KVWriteBatch();
  }

  @Override
  public void clear() {
    FileEntryKey fileEntryKeyStart = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(0)
        .build();
    FileEntryKey fileEntryKeyEnd = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(Long.MAX_VALUE)
        .build();
    mKVStoreMetaInterface.deleteFileEntryRange(fileEntryKeyStart, fileEntryKeyEnd);
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    FileEntryKey fileEntryKey = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(parentId)
        .setName(childName)
        .build();
    FileEntryValue fileEntryValue = FileEntryValue.newBuilder()
        .setMID(childId)
        .build();
    // TODO(yyong) figure out the format of metadata part
    // Need to check if the entry exists.
    // mKVStoreMetaInterface.createFileEntry(fileEntryKey, fileEntryValue);
    // So far only update, the caller should make sure the key not existing
    mKVStoreMetaInterface.updateFileEntry(fileEntryKey, fileEntryValue);
  }

  @Override
  public void removeChild(long parentId, String name) {
    mKVStoreMetaInterface.deleteFileEntry(FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(parentId)
        .setName(name)
        .build());
    mKVStoreMetaInterface.deleteFileCacheStatus(FileCacheStatusKey.newBuilder()
        .setTableType(KVStoreTable.FILE_CACHE_STATUS)
        .setParentID(parentId)
        .setName(name)
        .build());
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long parentId, String name, ReadOption option) {
    FileEntryKey key = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(parentId)
        .setName(name)
        .build();
    Optional<FileEntryValue> value = mKVStoreMetaInterface.getFileEntry(key);
    if (!value.isPresent()) {
      return Optional.empty();
    }

    return Optional.of(KVStoreUtils.convertToMutableInode(key, value.get()));
  }

  @Override
  public CloseableIterator<Pair<Long, String>> getChildIds(Long inodeId,
      ReadOption option) {
    FileEntryKey keyStart = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(inodeId)
        .setName("")
        .build();
    FileEntryKey keyEnd = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(inodeId + 1)
        .setName("")
        .build();
    List<Pair<FileEntryKey, FileEntryValue>> keys
        = mKVStoreMetaInterface.scanFileEntryKV(keyStart, keyEnd);
    KVStoreIter rocksIter = new KVStoreIter(keys.iterator());
    Stream<Pair<Long, String>> idStream = StreamSupport.stream(Spliterators
        .spliteratorUnknownSize(rocksIter, Spliterator.ORDERED), false);
    return CloseableIterator.create(idStream.iterator(), (any) -> {});
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name, ReadOption option) {
    FileEntryKey key = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(inodeId)
        .setName(name)
        .build();
    Optional<FileEntryValue> value
        = mKVStoreMetaInterface.getFileEntry(key);
    return value.isPresent() ? Optional.of(value.get().getMID()) : Optional.empty();
  }

  static class KVStoreIter implements Iterator<Pair<Long, String>> {
    final Iterator<Pair<FileEntryKey, FileEntryValue>> mIter;

    KVStoreIter(Iterator<Pair<FileEntryKey, FileEntryValue>> fileEntryIter) {
      mIter = fileEntryIter;
    }

    @Override
    public boolean hasNext() {
      return mIter.hasNext();
    }

    @Override
    public Pair<Long, String> next() {
      Pair<FileEntryKey, FileEntryValue> pair = mIter.next();
      return new Pair<Long, String>(pair.getFirst().getParentID(), pair.getFirst().getName());
    }
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String name, ReadOption option) {
    Optional<MutableInode<?>> mutableInode = getMutable(inodeId, name, option);
    if (!mutableInode.isPresent()) {
      return Optional.empty();
    }

    return Optional.of(Inode.wrap(mutableInode.get()));
  }

  @Override
  public boolean hasChildren(InodeDirectoryView inode, ReadOption option) {
    FileEntryKey keyStart = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(inode.getId())
        .setName("")
        .build();
    FileEntryKey keyEnd = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(inode.getId() + 1)
        .setName("")
        .build();
    List<Pair<FileEntryKey, FileEntryValue>> list
        = mKVStoreMetaInterface.scanFileEntryKV(keyStart, keyEnd, 1);
    return list.size() > 0;
  }

  @Override
  public boolean supportsBatchWrite() {
    return true;
  }

  private class KVWriteBatch implements WriteBatch {
    private final Map<ByteString, ByteString> mBatchAdd = new HashMap<>();
    private final List<ByteString> mBatchRemove = new LinkedList<>();

    private void addToMap(MutableInode<?> inode, Map<ByteString, ByteString> batch) {
      FileEntryKey key = FileEntryKey.newBuilder()
          .setTableType(KVStoreTable.FILE_ENTRY)
          .setParentID(inode.getParentId())
          .setName(inode.getName())
          .build();
      FileEntryValue.Builder valueBuilder = FileEntryValue.newBuilder()
          .setMID(inode.getId());

      if (inode instanceof MutableInodeDirectory) {
        return;
      }

      // File

      batch.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()),
          org.tikv.shade.com.google.protobuf.ByteString.copyFrom(valueBuilder.build().toByteArray()));
    }

    @Override
    public void writeInode(MutableInode<?> inode) {
      addToMap(inode, mBatchAdd);
    }

    @Override
    public void addChild(Long parentId, String childName, Long childId) {
      throw new NotSupportedException("Not supported");
    }

    @Override
    public void removeChild(Long parentId, String childName) {
      FileEntryKey key = FileEntryKey.newBuilder()
          .setTableType(KVStoreTable.FILE_ENTRY)
          .setName(childName).setParentID(parentId).build();
      FileCacheStatusKey cacheStatusKey = FileCacheStatusKey.newBuilder()
          .setTableType(KVStoreTable.FILE_CACHE_STATUS)
          .setParentID(parentId)
          .setName(childName)
          .build();
      mBatchRemove.add(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()));
      mBatchRemove.add(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
          cacheStatusKey.toByteArray()));
    }

    @Override
    public void commit() {
      if (mBatchRemove.size() > 0) {
        mKVStoreMetaInterface.deleteEntryBatchAtomic(mBatchRemove);
      }

      if (mBatchAdd.size() > 0) {
        mKVStoreMetaInterface.putEntryBatchAtomic(mBatchAdd);
      }
    }

    @Override
    public void close() {
    }
  }

  @Override
  public void close(){
    LOG.info("Closing TiKVInodeStore");
    try {
      mKVStoreInterface.close();
      LOG.info("TiKVInodeStore closed");
    } catch (IOException e) {
      LOG.info("TiKVInodeStore close exception {}", e);
    }
  }
}
