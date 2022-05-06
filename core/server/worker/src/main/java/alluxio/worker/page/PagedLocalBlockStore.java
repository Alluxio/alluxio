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

import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.LocalBlockStore;
import alluxio.worker.block.UfsInputStreamCache;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * A paged implementation of LocalBlockStore interface.
 * Implements the block level operations， but instead of using physical block files,
 * we use pages managed by the CacheManager to store the data.
 */
public class PagedLocalBlockStore implements LocalBlockStore {

  private final CacheManager mCacheManager;
  private final UfsManager mUfsManager;
  private PagedBlockMetaStore mPagedBlockMetaStore;
  private AlluxioConfiguration mConf;
  private final UfsInputStreamCache mUfsInStreamCache = new UfsInputStreamCache();

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
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<BlockMeta> getVolatileBlockMeta(long blockId)  {
    throw new UnsupportedOperationException();
  }

  @Override
  public TempBlockMeta getTempBlockMeta(long blockId) throws BlockDoesNotExistException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long commitBlockLocked(long sessionId, long blockId, boolean pinOnCreate)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abortBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockWriter getBlockWriter(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException {
    return null;
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset)
      throws BlockDoesNotExistException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockReader getBlockReader(long sessionId, long blockId,
                                    Protocol.OpenUfsBlockOptions options) {
    return new PagedBlockReader(mCacheManager, mUfsManager, mUfsInStreamCache, mConf, blockId,
        options);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, AllocateOptions moveOptions)
      throws BlockDoesNotExistException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
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
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updatePinnedInodes(Set<Long> inodes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeInaccessibleStorage() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
  }
}
