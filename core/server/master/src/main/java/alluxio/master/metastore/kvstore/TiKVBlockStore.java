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
import alluxio.grpc.BlockLocation;
import alluxio.kvstore.KVStoreBlockMeta;
import alluxio.kvstore.KVStoreFactory;
import alluxio.kvstore.KVStoreInterface;
import alluxio.kvstore.KVStoreMetaInterface;
import alluxio.kvstore.KVStoreMountInterface;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.metastore.BlockMetaStore;
import alluxio.master.metastore.KVInodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.proto.kvstore.BlockLocationKey;
import alluxio.proto.kvstore.BlockLocationValue;
import alluxio.proto.kvstore.FileCacheStatus;
import alluxio.proto.kvstore.FileCacheStatusKey;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.kvstore.InodeTreeEdgeKey;
import alluxio.proto.kvstore.InodeTreeEdgeValue;
import alluxio.proto.kvstore.KVEntryType;
import alluxio.proto.kvstore.KVStoreTable;
import alluxio.proto.meta.Block;
import alluxio.proto.meta.InodeMeta;
import alluxio.resource.CloseableIterator;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import javax.annotation.concurrent.ThreadSafe;
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
public class TiKVBlockStore implements BlockMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(TiKVBlockStore.class);

  private final KVStoreInterface mKVStoreInterface;
  private final KVStoreMetaInterface mKVStoreMetaInterface;
  private final KVStoreMountInterface mKVStoreMountInterface;
  private final KVStoreBlockMeta mKVStoreBLockMeta;

  /**
   * Creates and initializes a rocks block store.
   *
   */
  public TiKVBlockStore() {
    mKVStoreInterface = KVStoreFactory.getKVStoreFactory();
    mKVStoreMetaInterface = mKVStoreInterface.getMetaKVStore();
    mKVStoreMountInterface = mKVStoreInterface.getMountKVStore();
    mKVStoreBLockMeta = mKVStoreInterface.getBlockKVStore();
  }

  @Override
  public Optional<alluxio.proto.meta.Block.BlockMeta> getBlock(long id) {
    return Optional.empty();
  }

  @Override
  public void putBlock(long id, alluxio.proto.meta.Block.BlockMeta meta) {
    mKVStoreBLockMeta.putBlock(BlockLocationKey.newBuilder()
        .setTableType(KVStoreTable.BLOCK_LOCATION)
        .setFileId(id).build(),
        BlockLocationValue.newBuilder().setValue(meta.toByteString()).build());
  }

  @Override
  public void removeBlock(long id) {
    mKVStoreBLockMeta.removeBlock(BlockLocationKey.newBuilder()
        .setTableType(KVStoreTable.BLOCK_LOCATION)
        .setFileId(id)
        .build());
  }

  @Override
  public void clear() {
    mKVStoreBLockMeta.close();
  }

  @Override
  public List<alluxio.proto.meta.Block.BlockLocation> getLocations(long id) {
    List<BlockLocationValue>  blockLocationValues = mKVStoreBLockMeta
        .getLocations(BlockLocationKey.newBuilder()
        .setTableType(KVStoreTable.BLOCK_LOCATION)
        .setFileId(id)
        .build());
    List<alluxio.proto.meta.Block.BlockLocation> blockLocations = new LinkedList<>();
    try {
      for (BlockLocationValue blockLocationValue : blockLocationValues) {
        blockLocations.add(alluxio.proto.meta.Block.BlockLocation
            .parseFrom(blockLocationValue.getValue().toByteArray()));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    return blockLocations;
  }

  @Override
  public void addLocation(long id, alluxio.proto.meta.Block.BlockLocation location) {
    mKVStoreBLockMeta.addLocation(BlockLocationKey.newBuilder()
        .setTableType(KVStoreTable.BLOCK_LOCATION).setFileId(id)
        .setWorkerId(location.getWorkerId()).build(),
        BlockLocationValue.newBuilder().setValue(location.toByteString()).build());
  }

  @Override
  public void removeLocation(long blockId, long workerId) {
    mKVStoreBLockMeta.removeLocation(BlockLocationKey.newBuilder()
            .setTableType(KVStoreTable.BLOCK_LOCATION).setFileId(blockId)
            .setWorkerId(workerId).build());
  }

  @Override
  public void close(){
    LOG.info("Closing TiKVInodeStore");
    try {
      mKVStoreInterface.close();
      mKVStoreBLockMeta.close();
      LOG.info("TiKVInodeStore closed");
    } catch (IOException e) {
      LOG.info("TiKVInodeStore close exception {}", e);
    }
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public CloseableIterator<Block> getCloseableIterator() {
    Iterator<Block> iter = new Iterator<Block>() {
      BlockLocationKey mStartKey = BlockLocationKey.newBuilder()
          .setTableType(KVStoreTable.BLOCK_LOCATION)
          .setFileId(0)
          .build();
      Iterator<Block> mIter;
      boolean mFinished = false;

      @Override
      public boolean hasNext() {
        if (mIter != null && mIter.hasNext()) {
          return true;
        }
        if (mFinished) {
          return false;
        }

        List<org.tikv.kvproto.Kvrpcpb.KvPair> results = mKVStoreBLockMeta
            .scan(mStartKey.toByteArray(), 1000);
        List<Block> blocks = new LinkedList<>();
        BlockLocationKey blockLocationKey = null;
        try {
          for (Kvrpcpb.KvPair kvPair : results) {
            blockLocationKey = BlockLocationKey.parseFrom(kvPair.getKey().toByteArray());
            if (!blockLocationKey.hasWorkerId()) {
              continue;
            }
            BlockLocationValue blockLocationValue = BlockLocationValue
                .parseFrom(kvPair.getValue().toByteArray());
            blocks.add(new Block(blockLocationKey.getFileId(),
                alluxio.proto.meta.Block.BlockMeta.parseFrom(blockLocationValue.getValue())));
          }
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }

        if (blocks.isEmpty()) {
          mFinished = true;
          return false;
        }

        if (blockLocationKey != null && blockLocationKey.hasWorkerId()) {
          mStartKey = BlockLocationKey.newBuilder()
              .setTableType(KVStoreTable.BLOCK_LOCATION)
              .setFileId(blockLocationKey.getFileId())
              .setFileId(blockLocationKey.getFileId() + 1)
              .build();
        } else {
          mFinished = true;
        }

        mIter = blocks.stream().iterator();
        return true;
      }

      @Override
      public Block next() {
        return mIter.next();
      }
    };

    return new CloseableIterator<Block>(iter) {
      @Override
      public void closeResource() {
      }
    };
  }
}
