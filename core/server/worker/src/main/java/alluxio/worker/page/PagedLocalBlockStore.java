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

import static alluxio.worker.page.PagedBlockMetaStore.DEFAULT_DIR;
import static alluxio.worker.page.PagedBlockMetaStore.DEFAULT_TIER;

import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.LocalBlockStore;
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
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A paged implementation of LocalBlockStore interface.
 * Implements the block level operations， but instead of using physical block files,
 * we use pages managed by the CacheManager to store the data.
 */
public class PagedLocalBlockStore implements LocalBlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(PagedLocalBlockStore.class);

  private final CacheManager mCacheManager;
  private final UfsManager mUfsManager;
  private final PagedBlockMetaStore mPagedBlockMetaStore;

  /** A set of pinned inodes updated via periodic master-worker sync. */
  private final Set<Long> mPinnedInodes = new HashSet<>();
  private final AlluxioConfiguration mConf;
  private final UfsInputStreamCache mUfsInStreamCache = new UfsInputStreamCache();
  private final List<BlockStoreEventListener> mBlockStoreEventListeners =
      new CopyOnWriteArrayList<>();

  /**
   * Constructor for PagedLocalBlockStore.
   * @param cacheManager page cache manager
   * @param ufsManager ufs manager
   * @param pagedBlockMetaStore meta data store for pages and blocks
   * @param conf alluxio configurations
   */
  public PagedLocalBlockStore(CacheManager cacheManager, UfsManager ufsManager,
                              PagedBlockMetaStore pagedBlockMetaStore,
                              AlluxioConfiguration conf) {
    mCacheManager = cacheManager;
    mUfsManager = ufsManager;
    mPagedBlockMetaStore = pagedBlockMetaStore;
    mConf = conf;
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
  public TempBlockMeta createBlock(long sessionId, long blockId, AllocateOptions options)
      throws WorkerOutOfSpaceException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<BlockMeta> getVolatileBlockMeta(long blockId)  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<TempBlockMeta> getTempBlockMeta(long blockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate)
      throws IOException {
    // TODO(bowen): implement actual committing and replace placeholder values
    BlockStoreLocation dummyLoc = new BlockStoreLocation(DEFAULT_TIER, 1);
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onCommitBlock(sessionId, blockId, dummyLoc);
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public long commitBlockLocked(long sessionId, long blockId, boolean pinOnCreate)
      throws IOException {
    // TODO(bowen): implement actual committing and replace placeholder values
    BlockStoreLocation dummyLoc = new BlockStoreLocation(DEFAULT_TIER, 1);
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onCommitBlock(sessionId, blockId, dummyLoc);
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public void abortBlock(long sessionId, long blockId)
      throws IOException {
    // TODO(bowen): implement actual abortion and replace placeholder values
    boolean blockAborted = true;
    if (blockAborted) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onAbortBlock(sessionId, blockId);
        }
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws WorkerOutOfSpaceException, IOException {
    // TODO(bowen): implement actual space allocation and replace placeholder values
    boolean blockEvicted = true;
    if (blockEvicted) {
      long evictedBlockId = 0;
      BlockStoreLocation evictedBlockLocation = new BlockStoreLocation(DEFAULT_TIER, 1);
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onRemoveBlockByWorker(sessionId, evictedBlockId);
          listener.onRemoveBlock(sessionId, evictedBlockId, evictedBlockLocation);
        }
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId)
      throws IOException {
    return null;
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId,
                                       Protocol.OpenUfsBlockOptions options) {
    return new PagedBlockReader(mCacheManager, mUfsManager, mUfsInStreamCache, mConf, blockId,
        options);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, AllocateOptions moveOptions)
      throws WorkerOutOfSpaceException, IOException {
    // TODO(bowen): implement actual move and replace placeholder values
    BlockStoreLocation srcLocation = new BlockStoreLocation(DEFAULT_TIER, 1);
    BlockStoreLocation destLocation = new BlockStoreLocation(DEFAULT_TIER, 1);
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onMoveBlockByClient(sessionId, blockId, srcLocation, destLocation);
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
        listener.onRemoveBlockByClient(sessionId, blockId);
        if (removeSuccess) {
          BlockStoreLocation removedFrom = new BlockStoreLocation(DEFAULT_TIER, 1);
          listener.onRemoveBlock(sessionId, blockId, removedFrom);
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
          listener.onAccessBlock(sessionId, blockId);
          listener.onAccessBlock(sessionId, blockId, dummyLoc);
        }
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    return mPagedBlockMetaStore;
  }

  @Override
  public BlockStoreMeta getBlockStoreMetaFull() {
    return mPagedBlockMetaStore;
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
        String lostStorageTier = DEFAULT_TIER;
        String lostStoragePath = DEFAULT_DIR;
        BlockStoreLocation lostStoreLocation = new BlockStoreLocation(DEFAULT_TIER, 1);
        for (long lostBlock : lostBlocks) {
          listener.onBlockLost(lostBlock);
        }
        listener.onStorageLost(lostStorageTier, lostStoragePath);
        listener.onStorageLost(lostStoreLocation);
      }
    }
  }

  @Override
  public void close() throws IOException {
  }
}
