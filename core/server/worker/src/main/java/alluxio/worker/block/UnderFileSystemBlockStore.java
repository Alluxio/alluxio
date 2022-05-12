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

import alluxio.AlluxioURI;
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
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
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
public final class UnderFileSystemBlockStore implements SessionCleanable {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemBlockStore.class);

  /**
   * This lock protects mBlocks, mSessionIdToBlockIds and mBlockIdToSessionIds. For any read/write
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
  private final Map<Long, Set<ExistingBlock>> mSessionIdToBlocks = new HashMap<>();

  private final ConcurrentMap<BytesReadMetricKey, Counter> mUfsBytesReadMetrics =
      new ConcurrentHashMap<>();

  private final ConcurrentMap<AlluxioURI, Meter> mUfsBytesReadThroughputMetrics =
      new ConcurrentHashMap<>();

  /** The Local block store. */
  private final LocalBlockStore mLocalBlockStore;

  /** The manager for all ufs. */
  private final UfsManager mUfsManager;

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
   * Grants access for a UFS block given the session ID and its block ID.
   * If access to this block has been acquired during this session, empty is returned.
   * <p>
   * This is the only place where an {@link ExistingBlock} token is granted to client code.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @param options the options
   * @return an access token, otherwise empty if the token has already been acquired and not
   *         released by {@link #releaseAccess(ExistingBlock)}
   */
  public Optional<ExistingBlock> acquireAccess(
      long sessionId, long blockId, Protocol.OpenUfsBlockOptions options) {
    try (LockResource lr = new LockResource(mLock)) {
      Key key = new Key(sessionId, blockId);
      if (mBlocks.containsKey(key)) {
        return Optional.empty();
      }
      ExistingBlock block = new ExistingBlock(sessionId, blockId, options);
      mBlocks.put(key, new BlockInfo(block));
      Set<ExistingBlock> blocks =
          mSessionIdToBlocks.computeIfAbsent(sessionId, k -> new HashSet<>());
      blocks.add(block);
      return Optional.of(block);
    }
  }

  /**
   * Closes the block reader or writer and checks whether it is necessary to commit the block
   * to Local block store.
   *
   * During UFS block read, this is triggered when the block is unlocked.
   * During UFS block write, this is triggered when the UFS block is committed.
   *
   * @param block the block acquired
   *              from {@link #acquireAccess(long, long, Protocol.OpenUfsBlockOptions)}
   */
  public void close(ExistingBlock block) throws IOException {
    BlockInfo blockInfo;
    try (LockResource lr = new LockResource(mLock)) {
      blockInfo = mBlocks.get(new Key(block.getSessionId(), block.getBlockId()));
    }
    blockInfo.close();
  }

  /**
   * Releases the access token of this block.
   *
   * @param block the block acquired
   *              from {@link #acquireAccess(long, long, Protocol.OpenUfsBlockOptions)}
   */
  public void releaseAccess(ExistingBlock block) {
    try (LockResource lr = new LockResource(mLock)) {
      long sessionId = block.getSessionId();
      long blockId = block.getBlockId();
      Key key = new Key(sessionId, blockId);
      mBlocks.remove(key);
      Set<ExistingBlock> blocks = mSessionIdToBlocks.get(sessionId);
      if (blocks != null) {
        blocks.remove(block);
        if (blocks.isEmpty()) {
          mSessionIdToBlocks.remove(sessionId);
        }
      }
    }
  }

  /**
   * Cleans up and releases all the block information(e.g. block reader/writer)
   * that belongs to this session.
   *
   * @param sessionId the session ID
   */
  @Override
  public void cleanupSession(long sessionId) {
    Set<ExistingBlock> blocks;
    try (LockResource lr = new LockResource(mLock)) {
      blocks = mSessionIdToBlocks.get(sessionId);
      if (blocks == null) {
        return;
      }
    }
    // Note that, there can be a race condition that blocks can be stale when we release the
    // access. The race condition only has a minimal negative consequence (printing extra logging
    // message), and is expected very rare to trigger.
    for (ExistingBlock block : blocks) {
      try {
        // Note that we don't need to explicitly call abortBlock to cleanup the temp block
        // in Local block store because they will be cleanup by the session cleaner in the
        // Local block store.
        close(block);
        releaseAccess(block);
      } catch (Exception e) {
        LOG.warn("Failed to cleanup UFS block {}, session {}.", block, sessionId);
      }
    }
  }

  /**
   * Creates a block reader that reads from UFS and optionally caches the block to the Alluxio
   * block store.
   *
   * @param block the block to read
   * @param offset the read offset within the block (NOT the file)
   * @param positionShort whether the client op is a positioned read to a small buffer
   * @param options the open ufs options
   * @return the block reader instance
   */
  public BlockReader createBlockReader(ExistingBlock block, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    if (!options.hasUfsPath() && options.getBlockInUfsTier()) {
      // This is a fallback UFS block read. Reset the UFS block path according to the UfsBlock
      // flag.mUnderFileSystemBlockStore
      UfsManager.UfsClient ufsClient = mUfsManager.get(options.getMountId());
      options = options.toBuilder()
          .setUfsPath(alluxio.worker.BlockUtils.getUfsBlockPath(ufsClient, block.getBlockId()))
          .build();
    }
    final BlockInfo blockInfo;
    try (LockResource lr = new LockResource(mLock)) {
      blockInfo =  mBlocks.get(new Key(block.getSessionId(), block.getBlockId()));
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
   * A token for a block that has been granted access via
   * {@link #acquireAccess(long, long, Protocol.OpenUfsBlockOptions)}.
   */
  public static class ExistingBlock extends UnderFileSystemBlockMeta {
    /**
     * Private constructor to ensure only acquireAccess can create new instances.
     */
    private ExistingBlock(long sessionId, long blockId, Protocol.OpenUfsBlockOptions options) {
      super(sessionId, blockId, options);
    }
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
    private final ExistingBlock mMeta;

    private BlockReader mBlockReader;

    /**
     * Creates an instance of {@link BlockInfo}.
     *
     * @param meta the UFS block meta
     */
    public BlockInfo(ExistingBlock meta) {
      mMeta = meta;
    }

    /**
     * @return the UFS block meta
     */
    public ExistingBlock getMeta() {
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
