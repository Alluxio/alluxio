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

import alluxio.Constants;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.UfsBlockAccessTokenUnavailableException;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.meta.UfsBlockMeta;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

/**
 * This class manages the virtual blocks in the UFS for delegated UFS read/write.
 *
 * The usage pattern:
 * acquireAccess(blockMeta, maxConcurrency)
 * cleanup(sessionId, blockId)
 * releaseAccess(sessionId, blockId)
 */
public final class UfsBlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(UfsBlockStore.class);

  private final ReentrantLock mLock = new ReentrantLock();
  @GuardedBy("mLock")
  /** Maps from the {@link Key} to the {@link UfsBlockMeta}. */
  private final Map<Key, UfsBlockMeta> mBlocks = new HashMap<>();
  @GuardedBy("mLock")
  /** Maps from the session ID to the block IDs. */
  private final Map<Long, Set<Long>> mSessionIdToBlockIds = new HashMap<>();
  @GuardedBy("mLock")
  /** Maps from the block ID to the session IDs. */
  private final Map<Long, Set<Long>> mBlockIdToSessionIds = new HashMap<>();

  /** The Alluxio block store. */
  private final BlockStore mAlluxioBlockStore;

  /**
   * Creates an instance of {@link UfsBlockStore}.
   *
   * @param alluxioBlockStore the Alluxio block store
   */
  public UfsBlockStore(BlockStore alluxioBlockStore) {
    mAlluxioBlockStore = alluxioBlockStore;
  }

  /**
   * Acquires access for a UFS block given a {@link UfsBlockMeta} and the limit on the maximum
   * concurrent accessors on the block.
   *
   * @param blockMeta the block meta
   * @param maxConcurrency the maximum concurrency
   * @throws BlockAlreadyExistsException if the block already exists for a session ID
   * @throws UfsBlockAccessTokenUnavailableException if there are too many concurrent sessions
   *         accessing the block
   */
  public void acquireAccess(UfsBlockMeta blockMeta, int maxConcurrency) throws
      BlockAlreadyExistsException, UfsBlockAccessTokenUnavailableException {
    long sessionId = blockMeta.getSessionId();
    long blockId = blockMeta.getBlockId();
    mLock.lock();
    try {
      Key key = new Key(sessionId, blockId);
      if (mBlocks.containsKey(key)) {
        throw new BlockAlreadyExistsException(ExceptionMessage.UFS_BLOCK_ALREADY_EXISTS_FOR_SESSION,
            blockId, blockMeta.getUfsPath(), sessionId);
      }
      mBlocks.put(key, blockMeta);
      Set<Long> sessionIds = mBlockIdToSessionIds.get(blockId);
      if (sessionIds != null && sessionIds.size() >= maxConcurrency) {
        throw new UfsBlockAccessTokenUnavailableException(
            ExceptionMessage.UFS_BLOCK_ACCESS_TOKEN_UNAVAILABLE, sessionIds.size(), blockId,
            blockMeta.getUfsPath());
      }
      if (sessionIds == null) {
        sessionIds = new HashSet<>();
        mBlockIdToSessionIds.put(blockId, sessionIds);
      }
      sessionIds.add(sessionId);

      Set<Long> blockIds = mSessionIdToBlockIds.get(sessionId);
      if (blockIds == null) {
        blockIds = new HashSet<>();
        mSessionIdToBlockIds.put(sessionId, blockIds);
      }
      blockIds.add(blockId);
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Cleans up the block reader or writer.
   *
   * During UFS block read, this is triggered when the block is unlocked.
   * During UFS block write, this is triggered when the UFS block is committed.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @return true if block is to be committed into Alluxio block store
   * @throws IOException if it fails to clean up
   */
  public boolean cleanup(long sessionId, long blockId) throws IOException {
    UfsBlockMeta blockMeta;
    mLock.lock();
    try {
      blockMeta = mBlocks.get(new Key(sessionId, blockId));
      if (blockMeta == null) {
        return false;
      }
    } finally {
      mLock.unlock();
    }
    blockMeta.closeReaderOrWriter();
    return blockMeta.getCommitPending();
  }

  /**
   * Releases the access token of this block by removing this (sessionId, blockId) pair from the
   * store.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   */
  public void releaseAccess(long sessionId, long blockId) {
    mLock.lock();
    try {
      Key key = new Key(sessionId, blockId);
      mBlocks.remove(key);
      Set<Long> blockIds = mSessionIdToBlockIds.get(sessionId);
      if (blockIds != null) {
        blockIds.remove(blockId);
      }
      Set<Long> sessionIds = mBlockIdToSessionIds.get(blockId);
      if (sessionIds != null) {
        sessionIds.remove(sessionId);
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Cancels all the block information (e.g. block reader/writer) that belongs to this session.
   *
   * @param sessionId the session ID
   */
  public void cleanupSession(long sessionId) {
    Set<Long> blockIds;
    mLock.lock();
    try {
      blockIds = mSessionIdToBlockIds.get(sessionId);
      if (blockIds == null) {
        return;
      }
    } finally {
      mLock.unlock();
    }

    for (Long blockId : blockIds) {
      try {
        // Note that we don't need to explicitly call abortBlock to cleanup the temp block
        // in Alluxio block store because they will be cleanup by the session cleaner in the
        // Alluxio block store.
        cleanup(sessionId, blockId);
        releaseAccess(sessionId, blockId);
      } catch (Exception e) {
        LOG.warn("Failed to cleanup UFS block {}, session {}.", blockId, sessionId);
      }
    }
  }

  /**
   * Creates a UFS block reader for a UFS block.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @param offset the offset within the block (NOT the file)
   * @return the block reader instance
   * @throws BlockDoesNotExistException if the UFS block does not exist in the {@link UfsBlockStore}
   */
  public BlockReader getBlockReader(final long sessionId, long blockId, long offset,
      boolean noCache) throws BlockDoesNotExistException, IOException {
    final UfsBlockMeta blockMeta;
    mLock.lock();
    try {
      blockMeta = getBlockMeta(sessionId, blockId);
      if (blockMeta.getBlockReader() != null) {
        return blockMeta.getBlockReader();
      }
    } finally {
      mLock.unlock();
    }
    return new UfsBlockReader(blockMeta, offset, noCache, mAlluxioBlockStore);
  }

  /**
   * Gets the {@link UfsBlockMeta} for a session ID and block ID pair.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @return the {@link UfsBlockMeta} instance
   * @throws BlockDoesNotExistException if the UFS block does not exist in the {@link UfsBlockStore}
   */
  private UfsBlockMeta getBlockMeta(long sessionId, long blockId)
      throws BlockDoesNotExistException {
    Key key = new Key(sessionId, blockId);
    UfsBlockMeta blockMeta = mBlocks.get(key);
    if (blockMeta == null) {
      try {
        throw new BlockDoesNotExistException(ExceptionMessage.UFS_BLOCK_DOES_NOT_EXIST_FOR_SESSION,
            blockId, sessionId);
      } catch (Throwable e) {
        LOG.error("UFS Block does not exist.", e);
        throw e;
      }
    }
    return blockMeta;
  }

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
      return Objects.toStringHelper(this).add("blockId", mBlockId).add("sessionId", mSessionId)
          .toString();
    }
  }
}
