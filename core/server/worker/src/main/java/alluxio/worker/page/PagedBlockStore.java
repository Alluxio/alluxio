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

package alluxio.worker.page;

import static alluxio.worker.page.PagedBlockStoreMeta.DEFAULT_TIER;

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.UfsInputStreamCache;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A paged implementation of LocalBlockStore interface.
 * Implements the block level operations， but instead of using physical block files,
 * we use pages managed by the CacheManager to store the data.
 */
public class PagedBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(PagedBlockStore.class);

  private final CacheManager mCacheManager;
  private final UfsManager mUfsManager;
  private final PageMetaStore mPageMetaStore;
  /** A set of pinned inodes updated via periodic master-worker sync. */
  private final Set<Long> mPinnedInodes = new HashSet<>();
  private final AlluxioConfiguration mConf;
  private final UfsInputStreamCache mUfsInStreamCache = new UfsInputStreamCache();
  private final List<BlockStoreEventListener> mBlockStoreEventListeners =
      new CopyOnWriteArrayList<>();
  private final long mPageSize;

  /**
   * Create an instance of PagedBlockStore.
   * @param ufsManager
   * @return an instance of PagedBlockStore
   */
  public static PagedBlockStore create(UfsManager ufsManager) {
    try {
      AlluxioConfiguration conf = Configuration.global();
      PageMetaStore pageMetaStore = PageMetaStore.create(conf);
      CacheManager cacheManager =
          CacheManager.Factory.create(conf, pageMetaStore);
      return new PagedBlockStore(cacheManager, ufsManager, pageMetaStore, conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create PagedLocalBlockStore", e);
    }
  }

  /**
   * Constructor for PagedLocalBlockStore.
   * @param cacheManager page cache manager
   * @param ufsManager ufs manager
   * @param pageMetaStore meta data store for pages and blocks
   * @param conf alluxio configurations
   */
  PagedBlockStore(CacheManager cacheManager, UfsManager ufsManager,
                         PageMetaStore pageMetaStore,
                         AlluxioConfiguration conf) {
    mCacheManager = cacheManager;
    mUfsManager = ufsManager;
    mPageMetaStore = pageMetaStore;
    mConf = conf;
    mPageSize = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
  }

  @Override
  public OptionalLong pinBlock(long sessionId, long blockId) {
    return null;
  }

  @Override
  public void unpinBlock(long id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate) {
    // TODO(bowen): implement actual committing and replace placeholder values
    BlockStoreLocation dummyLoc = new BlockStoreLocation(DEFAULT_TIER, 1);
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onCommitBlock(blockId, dummyLoc);
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public String createBlock(long sessionId, long blockId, int tier,
      CreateBlockOptions createBlockOptions) {
    //TODO(Beinan): port the allocator algorithm from tiered block store
    PageStoreDir pageStoreDir = mPageMetaStore.getStoreDirs().get(
        Math.floorMod(Long.hashCode(blockId), mPageMetaStore.getStoreDirs().size()));
    pageStoreDir.putTempFile(String.valueOf(blockId));
    return "DUMMY_FILE_PATH";
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset,
                                       boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    return new PagedBlockReader(mCacheManager, mUfsManager, mUfsInStreamCache, mConf, blockId,
        options);
  }

  @Override
  public BlockReader createUfsBlockReader(long sessionId, long blockId, long offset,
                                          boolean positionShort,
                                          Protocol.OpenUfsBlockOptions options) throws IOException {
    return null;
  }

  @Override
  public void abortBlock(long sessionId, long blockId) {
    // TODO(bowen): implement actual abortion and replace placeholder values
    boolean blockAborted = true;
    if (blockAborted) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onAbortBlock(blockId);
        }
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes) {
    // TODO(bowen): implement actual space allocation and replace placeholder values
    boolean blockEvicted = true;
    if (blockEvicted) {
      long evictedBlockId = 0;
      BlockStoreLocation evictedBlockLocation = new BlockStoreLocation(DEFAULT_TIER, 1);
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onRemoveBlockByWorker(evictedBlockId);
          listener.onRemoveBlock(evictedBlockId, evictedBlockLocation);
        }
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<List<BlockStatus>> load(List<Block> fileBlocks, UfsReadOptions options) {
    return null;
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId)
      throws IOException {
    return new PagedBlockWriter(mCacheManager, blockId, mPageSize);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, AllocateOptions moveOptions)
      throws IOException {
    // TODO(bowen): implement actual move and replace placeholder values
    BlockStoreLocation srcLocation = new BlockStoreLocation(DEFAULT_TIER, 1);
    BlockStoreLocation destLocation = new BlockStoreLocation(DEFAULT_TIER, 1);
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onMoveBlockByClient(blockId, srcLocation, destLocation);
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeBlock(long sessionId, long blockId) throws IOException {
    LOG.debug("removeBlock: sessionId={}, blockId={}", sessionId, blockId);
    // TODO(bowen): implement actual removal and replace placeholder values
    boolean removeSuccess = true;
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onRemoveBlockByClient(blockId);
        if (removeSuccess) {
          BlockStoreLocation removedFrom = new BlockStoreLocation(DEFAULT_TIER, 1);
          listener.onRemoveBlock(blockId, removedFrom);
        }
      }
    }
  }

  @Override
  public void accessBlock(long sessionId, long blockId) {
    // TODO(bowen): implement actual access and replace placeholder values
    boolean blockExists = true;
    if (blockExists) {
      BlockStoreLocation dummyLoc = new BlockStoreLocation(DEFAULT_TIER, 1);
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onAccessBlock(blockId);
          listener.onAccessBlock(blockId, dummyLoc);
        }
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    return new PagedBlockStoreMeta(mPageMetaStore, false);
  }

  @Override
  public BlockStoreMeta getBlockStoreMetaFull() {
    return new PagedBlockStoreMeta(mPageMetaStore, true);
  }

  @Override
  public Optional<TempBlockMeta> getTempBlockMeta(long blockId) {
    return Optional.empty();
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasTempBlockMeta(long blockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<BlockMeta> getVolatileBlockMeta(long blockId) {
    return Optional.empty();
  }

  @Override
  public void cleanupSession(long sessionId) {
    // TODO(bowen): session cleaner seems to be defunct, as Sessions are always empty
  }

  @Override
  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    mBlockStoreEventListeners.add(listener);
  }

  @Override
  public void updatePinnedInodes(Set<Long> inodes) {
    // TODO(bowen): this is unused now, make sure to use the pinned inodes when allocating space
    LOG.debug("updatePinnedInodes: inodes={}", inodes);
    synchronized (mPinnedInodes) {
      mPinnedInodes.clear();
      mPinnedInodes.addAll(Preconditions.checkNotNull(inodes));
    }
  }

  @Override
  public void removeInaccessibleStorage() {
    // TODO(bowen): implement actual removal and replace placeholder values
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        List<Long> lostBlocks = ImmutableList.of();
        // TODO(bowen): lost directories can be obtained by iterating dirs in PageMetaStore
        // and check their health
        String lostStoragePath = "lostDir";
        BlockStoreLocation lostStoreLocation = new BlockStoreLocation(DEFAULT_TIER, 1);
        for (long lostBlock : lostBlocks) {
          listener.onBlockLost(lostBlock);
        }
        listener.onStorageLost(DEFAULT_TIER, lostStoragePath);
        listener.onStorageLost(lostStoreLocation);
      }
    }
  }

  @Override
  public void close() throws IOException {
  }
}
