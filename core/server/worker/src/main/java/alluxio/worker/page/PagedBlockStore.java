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

import static alluxio.worker.page.PagedBlockStoreMeta.DEFAULT_MEDIUM;
import static alluxio.worker.page.PagedBlockStoreMeta.DEFAULT_TIER;

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioRuntimeException;
import alluxio.exception.BlockDoesNotExistRuntimeException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.ErrorType;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
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
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A paged implementation of LocalBlockStore interface.
 * Implements the block level operations， but instead of using physical block files,
 * we use pages managed by the CacheManager to store the data.
 */
public class PagedBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(PagedBlockStore.class);

  private final CacheManager mCacheManager;
  private final UfsManager mUfsManager;
  private final PagedBlockMetaStore mPageMetaStore;
  private final BlockMasterClientPool mBlockMasterClientPool;
  private final AtomicReference<Long> mWorkerId;
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
   * @param pool
   * @param workerId
   * @return an instance of PagedBlockStore
   */
  public static PagedBlockStore create(UfsManager ufsManager, BlockMasterClientPool pool,
      AtomicReference<Long> workerId) {
    try {
      AlluxioConfiguration conf = Configuration.global();
      List<PageStoreDir> pageStoreDirs = PageStoreDir.createPageStoreDirs(conf);
      List<PagedBlockStoreDir> dirs = PagedBlockStoreDir.fromPageStoreDirs(pageStoreDirs);
      PagedBlockMetaStore pageMetaStore = new PagedBlockMetaStore(dirs);
      CacheManager cacheManager =
          CacheManager.Factory.create(conf, pageMetaStore);
      return new PagedBlockStore(cacheManager, ufsManager, pool, workerId, pageMetaStore, conf);
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
  PagedBlockStore(CacheManager cacheManager, UfsManager ufsManager, BlockMasterClientPool pool,
      AtomicReference<Long> workerId, PagedBlockMetaStore pageMetaStore,
      AlluxioConfiguration conf) {
    mCacheManager = cacheManager;
    mUfsManager = ufsManager;
    mBlockMasterClientPool = pool;
    mWorkerId = workerId;
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
    PagedBlockStoreDir pageStoreDir = (PagedBlockStoreDir)
        mPageMetaStore.allocate(String.valueOf(blockId), 0);
    // todo(bowen): need to pin this block until commit is complete
    try {
      pageStoreDir.commit(String.valueOf(blockId));
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
    BlockMasterClient bmc = mBlockMasterClientPool.acquire();
    try {
      bmc.commitBlock(mWorkerId.get(), mPageMetaStore.getStoreMeta().getUsedBytes(), DEFAULT_TIER,
          DEFAULT_MEDIUM, blockId, pageStoreDir.getBlockCachedBytes(blockId));
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.UNAVAILABLE,
          ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e, ErrorType.Internal,
          false);
    } finally {
      mBlockMasterClientPool.release(bmc);
    }
    BlockStoreLocation blockLocation =
        new BlockStoreLocation(DEFAULT_TIER, getDirIndexOfBlock(blockId));
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onCommitBlock(blockId, blockLocation);
      }
    }
  }

  @Override
  public String createBlock(long sessionId, long blockId, int tier,
      CreateBlockOptions createBlockOptions) throws WorkerOutOfSpaceException, IOException {
    PageStoreDir pageStoreDir =
        mPageMetaStore.allocate(String.valueOf(blockId), createBlockOptions.getInitialBytes());
    pageStoreDir.putTempFile(String.valueOf(blockId));
    return "DUMMY_FILE_PATH";
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset,
                                       boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {

    return new PagedBlockReader(mCacheManager,
        mUfsManager, mUfsInStreamCache, mConf, blockId, options);
  }

  @Override
  public BlockReader createUfsBlockReader(long sessionId, long blockId, long offset,
                                          boolean positionShort,
                                          Protocol.OpenUfsBlockOptions options) throws IOException {
    return null;
  }

  @Override
  public void abortBlock(long sessionId, long blockId) {
    PageStoreDir pageStoreDir =
        mPageMetaStore.allocate(String.valueOf(blockId), 0);
    try {
      pageStoreDir.abort(String.valueOf(blockId));
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onAbortBlock(blockId);
      }
    }
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
          listener.onRemoveBlockByWorker(evictedBlockId);
          listener.onRemoveBlock(evictedBlockId, evictedBlockLocation);
        }
      }
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public List<BlockStatus> load(List<Block> fileBlocks, String tag, OptionalLong bandwidth) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId)
      throws IOException {
    return new PagedBlockWriter(mCacheManager, blockId, mPageSize);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, AllocateOptions moveOptions)
      throws WorkerOutOfSpaceException, IOException {
    // TODO(bowen): implement actual move and replace placeholder values
    int dirIndex = getDirIndexOfBlock(blockId);
    BlockStoreLocation srcLocation = new BlockStoreLocation(DEFAULT_TIER, dirIndex);
    BlockStoreLocation destLocation = moveOptions.getLocation();
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
    int dirIndex = getDirIndexOfBlock(blockId);
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onRemoveBlockByClient(blockId);
        if (removeSuccess) {
          BlockStoreLocation removedFrom = new BlockStoreLocation(DEFAULT_TIER, dirIndex);
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
      int dirIndex = getDirIndexOfBlock(blockId);
      BlockStoreLocation dummyLoc = new BlockStoreLocation(DEFAULT_TIER, dirIndex);
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
    return mPageMetaStore.getStoreMeta();
  }

  @Override
  public BlockStoreMeta getBlockStoreMetaFull() {
    return mPageMetaStore.getStoreMetaFull();
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
    mPageMetaStore.registerBlockStoreEventListener(listener);
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

  private int getDirIndexOfBlock(long blockId) {
    int dirIndex = mPageMetaStore.getDirOfBlock(blockId)
        .orElseThrow(() -> new BlockDoesNotExistRuntimeException(
            String.format("Block %s does not exist", blockId)))
        .getDirIndex();
    return dirIndex;
  }
}
