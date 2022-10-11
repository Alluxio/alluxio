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

package alluxio.worker.block;

import static java.lang.String.format;

import alluxio.AlluxioURI;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.BlockDoesNotExistRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.LockResource;
import alluxio.underfs.UfsManager;
import alluxio.worker.SessionCleanable;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

/**
 * This class manages the virtual blocks in the UFS for delegated UFS reads/writes.
 *
 * The usage pattern:
 *  acquireAccess(sessionId, blockId, options)
 *  closeReaderOrWriter(sessionId, blockId)
 *  releaseAccess(sessionId, blockId)
 *
 * If the client is lost before releasing or cleaning up the session, the session cleaner will
 * clean the data.
 */
public final class UnderFileSystemBlockStore implements SessionCleanable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemBlockStore.class);

  /**
   * This lock protects mBlocks, mSessionIdToBlockIds. For any read/write
   * operations to these maps, the lock needs to be acquired. But once you get the block
   * information from the map (e.g. mBlocks), the lock does not need to be acquired. For example,
   * the block reader/writer within the BlockInfo can be updated without acquiring this lock.
   * This is based on the assumption that one session won't open multiple readers/writers on the
   * same block. If the client do that, the client can see failures but the worker won't crash.
   */
  private final ReentrantLock mLock = new ReentrantLock();
  /** Maps from the {@link Key} to the {@link BlockInfo}. */
  @GuardedBy("mLock")
  private final Map<Key, BlockInfo> mBlocks = new HashMap<>();
  /** Maps from the session ID to the block IDs. */
  @GuardedBy("mLock")
  private final Map<Long, Set<Long>> mSessionIdToBlockIds = new HashMap<>();

  private final ConcurrentMap<BytesReadMetricKey, Counter> mUfsBytesReadMetrics =
      new ConcurrentHashMap<>();

  private final ConcurrentMap<AlluxioURI, Meter> mUfsBytesReadThroughputMetrics =
      new ConcurrentHashMap<>();

  /** The Local block store. */
  private final LocalBlockStore mLocalBlockStore;

  /** The manager for all ufs. */
  private final UfsManager mUfsManager;

    /** The manager for all ufs. */
  private final ConcurrentMap<Long, UfsIOManager> mUfsIOManager = new ConcurrentHashMap<>();

  /** The cache for all ufs instream. */
  private final UfsInputStreamCache mUfsInstreamCache;

  /**
   * Creates an instance of {@link UnderFileSystemBlockStore}.
   *
   * @param localBlockStore the local block store
   * @param ufsManager the file manager
   */
  public UnderFileSystemBlockStore(LocalBlockStore localBlockStore, UfsManager ufsManager) {
    mLocalBlockStore = localBlockStore;
    mUfsManager = ufsManager;
    mUfsInstreamCache = new UfsInputStreamCache();
  }

  /**
   * Acquires access for a UFS block given a {@link UnderFileSystemBlockMeta} and the limit on
   * the maximum concurrency on the block. If the number of concurrent readers on this UFS block
   * exceeds a threshold, the token is not granted and this method returns false.
   *
   * @param sessionId the session ID
   * @param blockId maximum concurrency
   * @param options the options
   * @return whether an access token is acquired
   * @throws BlockAlreadyExistsException if the block already exists for a session ID
   */
  @VisibleForTesting
  public boolean acquireAccess(long sessionId, long blockId, Protocol.OpenUfsBlockOptions options)
      throws BlockAlreadyExistsException {
    UnderFileSystemBlockMeta blockMeta = new UnderFileSystemBlockMeta(sessionId, blockId, options);
    try (LockResource lr = new LockResource(mLock)) {
      Key key = new Key(sessionId, blockId);
      if (mBlocks.containsKey(key)) {
        throw new BlockAlreadyExistsException(MessageFormat.format(
            "UFS block {0,number,#} from UFS file {1} exists for session {2,number,#}", blockId,
            blockMeta.getUnderFileSystemPath(), sessionId));
      }
      mBlocks.put(key, new BlockInfo(blockMeta));
      Set<Long> blockIds = mSessionIdToBlockIds.computeIfAbsent(sessionId, k -> new HashSet<>());
      blockIds.add(blockId);
    }
    return true;
  }

  /**
   * Closes the block reader or writer and checks whether it is necessary to commit the block
   * to Local block store.
   *
   * During UFS block read, this is triggered when the block is unlocked.
   * During UFS block write, this is triggered when the UFS block is committed.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   */
  public void closeBlock(long sessionId, long blockId) throws IOException {
    BlockInfo blockInfo;
    try (LockResource lr = new LockResource(mLock)) {
      blockInfo = mBlocks.get(new Key(sessionId, blockId));
      if (blockInfo == null) {
        LOG.warn("Key (block ID: {}, session ID {}) is not found when cleaning up the UFS block.",
            blockId, sessionId);
        return;
      }
    }
    blockInfo.close();
  }

  /**
   * Releases the access token of this block by removing this (sessionId, blockId) pair from the
   * store.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   */
  public void releaseAccess(long sessionId, long blockId) {
    try (LockResource lr = new LockResource(mLock)) {
      Key key = new Key(sessionId, blockId);
      if (!mBlocks.containsKey(key)) {
        LOG.warn("Key (block ID: {}, session ID {}) is not found when releasing the UFS block.",
            blockId, sessionId);
      }
      mBlocks.remove(key);
      Set<Long> blockIds = mSessionIdToBlockIds.get(sessionId);
      if (blockIds != null) {
        blockIds.remove(blockId);
        if (blockIds.isEmpty()) {
          mSessionIdToBlockIds.remove(sessionId);
        }
      }
    }
  }

  /**
   * Cleans up all the block information(e.g. block reader/writer) that belongs to this session.
   *
   * @param sessionId the session ID
   */
  @Override
  public void cleanupSession(long sessionId) {
    Set<Long> blockIds;
    try (LockResource lr = new LockResource(mLock)) {
      blockIds = mSessionIdToBlockIds.get(sessionId);
      if (blockIds == null) {
        return;
      }
    }
    // Note that, there can be a race condition that blockIds can be stale when we release the
    // access. The race condition only has a minimal negative consequence (printing extra logging
    // message), and is expected very rare to trigger.
    for (Long blockId : blockIds) {
      try {
        // Note that we don't need to explicitly call abortBlock to cleanup the temp block
        // in Local block store because they will be cleanup by the session cleaner in the
        // Local block store.
        closeBlock(sessionId, blockId);
        releaseAccess(sessionId, blockId);
      } catch (Exception e) {
        LOG.warn("Failed to cleanup UFS block {}, session {}.", blockId, sessionId);
      }
    }
  }

  @Override
  public void close() throws IOException {
    mUfsIOManager.forEach((key, value) -> value.close());
  }

  /**
   * Creates a block reader that reads from UFS and optionally caches the block to the Alluxio
   * block store.
   *
   * @param sessionId the client session ID that requested this read
   * @param blockId the ID of the block to read
   * @param offset the read offset within the block (NOT the file)
   * @param positionShort whether the client op is a positioned read to a small buffer
   * @param options the open ufs options
   * @return the block reader instance
   * {@link UnderFileSystemBlockStore}
   */
  public BlockReader createBlockReader(final long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException, BlockAlreadyExistsException {
    if (!options.hasUfsPath() && options.getBlockInUfsTier()) {
      // This is a fallback UFS block read. Reset the UFS block path according to the UfsBlock
      // flag.mUnderFileSystemBlockStore
      UfsManager.UfsClient ufsClient = mUfsManager.get(options.getMountId());
      options = options.toBuilder()
          .setUfsPath(alluxio.worker.BlockUtils.getUfsBlockPath(ufsClient, blockId)).build();
    }
    acquireAccess(sessionId, blockId, options);
    final BlockInfo blockInfo;
    try (LockResource lr = new LockResource(mLock)) {
      blockInfo = getBlockInfo(sessionId, blockId);
      BlockReader blockReader = blockInfo.getBlockReader();
      if (blockReader != null) {
        return blockReader;
      }
    }
    UfsManager.UfsClient ufsClient = mUfsManager.get(blockInfo.getMeta().getMountId());
    Counter ufsBytesRead = mUfsBytesReadMetrics.computeIfAbsent(
        new BytesReadMetricKey(ufsClient.getUfsMountPointUri(), options.getUser()),
        key -> key.mUser == null
            ? MetricsSystem.counterWithTags(
                MetricKey.WORKER_BYTES_READ_UFS.getName(),
                MetricKey.WORKER_BYTES_READ_UFS.isClusterAggregated(),
                MetricInfo.TAG_UFS, MetricsSystem.escape(key.mUri))
            : MetricsSystem.counterWithTags(
                MetricKey.WORKER_BYTES_READ_UFS.getName(),
                MetricKey.WORKER_BYTES_READ_UFS.isClusterAggregated(),
                MetricInfo.TAG_UFS, MetricsSystem.escape(key.mUri),
                MetricInfo.TAG_USER, key.mUser));
    Meter ufsBytesReadThroughput = mUfsBytesReadThroughputMetrics.computeIfAbsent(
        ufsClient.getUfsMountPointUri(),
        uri -> MetricsSystem.meterWithTags(
              MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.getName(),
              MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.isClusterAggregated(),
              MetricInfo.TAG_UFS,
              MetricsSystem.escape(uri)));
    BlockReader reader =
        UnderFileSystemBlockReader.create(blockInfo.getMeta(), offset, positionShort,
            mLocalBlockStore, ufsClient, mUfsInstreamCache, ufsBytesRead, ufsBytesReadThroughput);
    blockInfo.setBlockReader(reader);
    return reader;
  }

  /**
   * Get ufsIOManager for the mount or add if absent.
   * @param mountId mount identifier
   * @return ufsIOManager for the mount
   */
  public UfsIOManager getOrAddUfsIOManager(long mountId) {
    return mUfsIOManager.computeIfAbsent(mountId, id -> {
      try {
        UfsIOManager manager = new UfsIOManager(mUfsManager.get(mountId));
        manager.start();
        return manager;
      } catch (AlluxioStatusException e) {
        throw AlluxioRuntimeException.from(e);
      }
    });
  }

  /**
   * @param sessionId the session ID
   * @param blockId the block ID
   * @return true if mNoCache is set
   */
  public boolean isNoCache(long sessionId, long blockId) {
    final BlockInfo blockInfo;
    try (LockResource lr = new LockResource(mLock)) {
      blockInfo = getBlockInfo(sessionId, blockId);
    }
    return blockInfo.getMeta().isNoCache();
  }

  /**
   * Gets the {@link UnderFileSystemBlockMeta} for a session ID and block ID pair.
   * The caller must have acquired the lock before calling this method.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @return the {@link UnderFileSystemBlockMeta} instance
   * @throws BlockDoesNotExistRuntimeException if the UFS block does not exist in the
   * {@link UnderFileSystemBlockStore}
   */
  @GuardedBy("mLock")
  private BlockInfo getBlockInfo(long sessionId, long blockId) {
    Key key = new Key(sessionId, blockId);
    BlockInfo blockInfo = mBlocks.get(key);
    if (blockInfo == null) {
      throw new NotFoundRuntimeException(format(
          "UFS block %s does not exist for session %s",  blockId, sessionId));
    }
    return blockInfo;
  }

  /**
   * This class is to wrap session ID amd block ID.
   */
  private static class Key {
    private final long mSessionId;
    private final long mBlockId;

    /**
     * Creates an instance of the Key class.
     *
     * @param sessionId the session ID
     * @param blockId the block ID
     */
    public Key(long sessionId, long blockId) {
      mSessionId = sessionId;
      mBlockId = blockId;
    }

    /**
     * @return the block ID
     */
    public long getBlockId() {
      return mBlockId;
    }

    /**
     * @return the session ID
     */
    public long getSessionId() {
      return mSessionId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }

      Key that = (Key) o;
      return Objects.equal(mBlockId, that.mBlockId) && Objects.equal(mSessionId, that.mSessionId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mBlockId, mSessionId);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("blockId", mBlockId).add("sessionId", mSessionId)
          .toString();
    }
  }

  private static class BytesReadMetricKey {
    private final AlluxioURI mUri;
    private final String mUser;

    BytesReadMetricKey(AlluxioURI uri, String user) {
      mUri = uri;
      mUser = user;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      BytesReadMetricKey that = (BytesReadMetricKey) o;
      return mUri.equals(that.mUri) && mUser.equals(that.mUser);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mUri, mUser);
    }
  }

  /**
   * This class is to wrap block reader/writer and the block meta into one class. The block
   * reader/writer is not part of the {@link UnderFileSystemBlockMeta} because
   * 1. UnderFileSystemBlockMeta only keeps immutable information.
   * 2. We do not want a cyclic dependency between {@link UnderFileSystemBlockReader} and
   *    {@link UnderFileSystemBlockMeta}.
   */
  private static class BlockInfo {
    private final UnderFileSystemBlockMeta mMeta;

    private BlockReader mBlockReader;

    /**
     * Creates an instance of {@link BlockInfo}.
     *
     * @param meta the UFS block meta
     */
    public BlockInfo(UnderFileSystemBlockMeta meta) {
      mMeta = meta;
    }

    /**
     * @return the UFS block meta
     */
    public UnderFileSystemBlockMeta getMeta() {
      return mMeta;
    }

    /**
     * @return the cached the block reader if it is not closed
     */
    public synchronized BlockReader getBlockReader() {
      if (mBlockReader != null && mBlockReader.isClosed()) {
        mBlockReader = null;
      }
      return mBlockReader;
    }

    /**
     * @param blockReader the block reader to be set
     */
    public synchronized void setBlockReader(BlockReader blockReader) {
      mBlockReader = blockReader;
    }

    /**
     * Closes the block reader or writer.
     */
    public synchronized void close() throws IOException {
      if (mBlockReader != null) {
        mBlockReader.close();
        mBlockReader = null;
      }
    }
  }
}
