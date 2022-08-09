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

package alluxio.worker.block.meta;

import static com.google.common.base.Preconditions.checkState;

import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.util.io.FileUtils;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents a directory in a storage tier. It has a fixed capacity allocated to it on
 * instantiation. It contains the set of blocks currently in the storage directory.
 *
 * Portion of capacity will be accounted as reserved space.
 * Through {@link StorageDirView}, this space will be reflected as:
 * - committed for user I/Os
 * - available for internal I/Os.
 */
@NotThreadSafe
public final class DefaultStorageDir implements StorageDir {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageDir.class);

  private final long mCapacityBytes;
  private final String mDirMedium;
  /** A map from block id to block metadata. */
  private final Map<Long, BlockMeta> mBlockIdToBlockMap = new HashMap<>(200);
  /** A map from block id to temp block metadata. */
  private final Map<Long, TempBlockMeta> mBlockIdToTempBlockMap = new HashMap<>(200);
  /** A map from session id to the set of temp blocks created by this session. */
  private final Map<Long, Set<Long>> mSessionIdToTempBlockIdsMap = new HashMap<>(200);
  private final AtomicLong mAvailableBytes;
  private final AtomicLong mCommittedBytes;
  private final AtomicLong mReservedBytes;
  private final String mDirPath;
  private final int mDirIndex;
  private final StorageTier mTier;

  private DefaultStorageDir(StorageTier tier, int dirIndex, long capacityBytes, long reservedBytes,
      String dirPath, String dirMedium) {
    mTier = Preconditions.checkNotNull(tier, "tier");
    mDirIndex = dirIndex;
    mCapacityBytes = capacityBytes;
    mReservedBytes = new AtomicLong(reservedBytes);
    mAvailableBytes = new AtomicLong(capacityBytes - reservedBytes);
    mCommittedBytes = new AtomicLong(0);
    mDirPath = dirPath;
    mDirMedium = dirMedium;
  }

  /**
   * Factory method to create {@link StorageDir}.
   *
   * It will load metadata of existing committed blocks in the dirPath specified. Only files with
   * directory depth 1 under dirPath and whose file name can be parsed into {@code long} will be
   * considered as existing committed blocks, these files will be preserved, others files or
   * directories will be deleted.
   *
   * @param tier the {@link StorageTier} this dir belongs to
   * @param dirIndex the index of this dir in its tier
   * @param capacityBytes the initial capacity of this dir, can not be modified later
   * @param reservedBytes the amount of reserved space for internal management
   * @param dirPath filesystem path of this dir for actual storage
   * @param dirMedium the medium type of the storage dir
   * @return the new created {@link StorageDir}
   */
  public static StorageDir newStorageDir(StorageTier tier, int dirIndex, long capacityBytes,
      long reservedBytes, String dirPath, String dirMedium) {
    DefaultStorageDir dir =
        new DefaultStorageDir(tier, dirIndex, capacityBytes, reservedBytes, dirPath, dirMedium);
    dir.initializeMeta();
    LOG.info("StorageDir initialized: path={}, tier={}, dirIndex={}, medium={}, capacityBytes={}, "
            + "reservedBytes={}, availableBytes={}",
        dirPath, tier, dirIndex, dirMedium, capacityBytes, reservedBytes, dir.mAvailableBytes);
    return dir;
  }

  /**
   * Initializes metadata for existing blocks in this {@link StorageDir}.
   *
   * Only paths satisfying the contract defined in
   * {@link DefaultBlockMeta#commitPath(StorageDir, long)} are legal, should be in format like
   * {dir}/{blockId}. other paths will be deleted.
   */
  private void initializeMeta() {
    // Create the storage directory path
    boolean isDirectoryNewlyCreated = FileUtils.createStorageDirPath(mDirPath,
        Configuration.getString(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS));
    String tmpDir = Paths.get(Configuration.getString(PropertyKey.WORKER_DATA_TMP_FOLDER))
        .getName(0).toString();
    if (isDirectoryNewlyCreated) {
      LOG.info("Folder {} was created!", mDirPath);
    }

    File dir = new File(mDirPath);
    File[] paths = dir.listFiles();
    if (paths == null) {
      return;
    }
    for (File path : paths) {
      if (!path.isFile()) {
        if (!path.getName().equals(tmpDir)) {
          LOG.error("{} in StorageDir is not a file", path.getAbsolutePath());
        }
        try {
          // TODO(calvin): Resolve this conflict in class names.
          org.apache.commons.io.FileUtils.deleteDirectory(path);
        } catch (IOException e) {
          LOG.error("can not delete directory {}", path.getAbsolutePath(), e);
        }
      } else {
        try {
          long blockId = Long.parseLong(path.getName());
          addBlockMeta(new DefaultBlockMeta(blockId, path.length(), this));
        } catch (NumberFormatException e) {
          LOG.error("filename of {} in StorageDir can not be parsed into long",
              path.getAbsolutePath(), e);
          if (path.delete()) {
            LOG.warn("file {} has been deleted", path.getAbsolutePath());
          } else {
            LOG.error("can not delete file {}", path.getAbsolutePath());
          }
        }
      }
    }
  }

  @Override
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  @Override
  public long getAvailableBytes() {
    return mAvailableBytes.get();
  }

  @Override
  public long getCommittedBytes() {
    return mCommittedBytes.get();
  }

  @Override
  public String getDirPath() {
    return mDirPath;
  }

  @Override
  public String getDirMedium() {
    return mDirMedium;
  }

  @Override
  public StorageTier getParentTier() {
    return mTier;
  }

  @Override
  public int getDirIndex() {
    return mDirIndex;
  }

  @Override
  public List<Long> getBlockIds() {
    return new ArrayList<>(mBlockIdToBlockMap.keySet());
  }

  @Override
  public List<BlockMeta> getBlocks() {
    return new ArrayList<>(mBlockIdToBlockMap.values());
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    return mBlockIdToBlockMap.containsKey(blockId);
  }

  @Override
  public boolean hasTempBlockMeta(long blockId) {
    return mBlockIdToTempBlockMap.containsKey(blockId);
  }

  @Override
  public Optional<BlockMeta> getBlockMeta(long blockId) {
    return Optional.ofNullable(mBlockIdToBlockMap.get(blockId));
  }

  @Override
  public Optional<TempBlockMeta> getTempBlockMeta(long blockId) {
    return Optional.ofNullable(mBlockIdToTempBlockMap.get(blockId));
  }

  @Override
  public void addBlockMeta(BlockMeta blockMeta) {
    Preconditions.checkNotNull(blockMeta, "blockMeta");
    long blockId = blockMeta.getBlockId();
    long blockSize = blockMeta.getBlockSize();
    if (getAvailableBytes() + getReservedBytes() < blockSize) {
      throw new ResourceExhaustedRuntimeException(
          ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(blockId, blockSize,
              getAvailableBytes(), blockMeta.getBlockLocation().tierAlias()), false);
    }
    checkState(!hasBlockMeta(blockId), ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(blockId,
        blockMeta.getBlockLocation().tierAlias()));
    mBlockIdToBlockMap.put(blockId, blockMeta);
    reserveSpace(blockSize, true);
  }

  @Override
  public void addTempBlockMeta(TempBlockMeta tempBlockMeta) {
    Preconditions.checkNotNull(tempBlockMeta, "tempBlockMeta");
    long sessionId = tempBlockMeta.getSessionId();
    long blockId = tempBlockMeta.getBlockId();
    long blockSize = tempBlockMeta.getBlockSize();
    if (getAvailableBytes() + getReservedBytes() < blockSize) {
      throw new ResourceExhaustedRuntimeException(
          ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(blockId, blockSize,
              getAvailableBytes(), tempBlockMeta.getBlockLocation().tierAlias()), false);
    }
    checkState(!hasTempBlockMeta(blockId), ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(blockId,
        tempBlockMeta.getBlockLocation().tierAlias()));
    mBlockIdToTempBlockMap.put(blockId, tempBlockMeta);
    Set<Long> sessionTempBlocks = mSessionIdToTempBlockIdsMap.get(sessionId);
    if (sessionTempBlocks == null) {
      mSessionIdToTempBlockIdsMap.put(sessionId, Sets.newHashSet(blockId));
    } else {
      sessionTempBlocks.add(blockId);
    }
    reserveSpace(blockSize, false);
  }

  @Override
  public void removeBlockMeta(BlockMeta blockMeta) {
    Preconditions.checkNotNull(blockMeta, "blockMeta");
    long blockId = blockMeta.getBlockId();
    BlockMeta deletedBlockMeta = mBlockIdToBlockMap.remove(blockId);
    if (deletedBlockMeta != null) {
      reclaimSpace(blockMeta.getBlockSize(), true);
    }
  }

  @Override
  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH")
  public void removeTempBlockMeta(TempBlockMeta tempBlockMeta) {
    Preconditions.checkNotNull(tempBlockMeta, "tempBlockMeta");
    final long blockId = tempBlockMeta.getBlockId();
    final long sessionId = tempBlockMeta.getSessionId();
    TempBlockMeta deletedTempBlockMeta = mBlockIdToTempBlockMap.remove(blockId);
    checkState(deletedTempBlockMeta != null,
        ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(blockId));
    Set<Long> sessionBlocks = mSessionIdToTempBlockIdsMap.get(sessionId);
    checkState(sessionBlocks != null && sessionBlocks.remove(blockId),
        ExceptionMessage.BLOCK_NOT_FOUND_FOR_SESSION.getMessage(blockId, mTier.getTierAlias(),
            sessionId));
    if (sessionBlocks.isEmpty()) {
      mSessionIdToTempBlockIdsMap.remove(sessionId);
    }
    reclaimSpace(tempBlockMeta.getBlockSize(), false);
  }

  @Override
  public void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize) {
    long oldSize = tempBlockMeta.getBlockSize();
    checkState(oldSize < newSize, "Shrinking block, not supported!");
    if (newSize > oldSize) {
      reserveSpace(newSize - oldSize, false);
      tempBlockMeta.setBlockSize(newSize);
    }
  }

  @Override
  public void cleanupSessionTempBlocks(long sessionId, List<Long> tempBlockIds) {
    Set<Long> sessionTempBlocks = mSessionIdToTempBlockIdsMap.get(sessionId);
    // The session's temporary blocks have already been removed.
    if (sessionTempBlocks == null) {
      return;
    }
    for (Long tempBlockId : tempBlockIds) {
      if (!mBlockIdToTempBlockMap.containsKey(tempBlockId)) {
        // This temp block does not exist in this dir, this is expected for some blocks since the
        // input list is across all dirs
        continue;
      }
      sessionTempBlocks.remove(tempBlockId);
      TempBlockMeta tempBlockMeta = mBlockIdToTempBlockMap.remove(tempBlockId);
      if (tempBlockMeta != null) {
        reclaimSpace(tempBlockMeta.getBlockSize(), false);
      } else {
        LOG.error("Cannot find blockId {} when cleanup sessionId {}", tempBlockId, sessionId);
      }
    }
    if (sessionTempBlocks.isEmpty()) {
      mSessionIdToTempBlockIdsMap.remove(sessionId);
    } else {
      // This may happen if the client comes back during clean up and creates more blocks or some
      // temporary blocks failed to be deleted
      LOG.warn("Blocks still owned by session {} after cleanup.", sessionId);
    }
  }

  @Override
  public List<TempBlockMeta> getSessionTempBlocks(long sessionId) {
    Set<Long> sessionTempBlockIds = mSessionIdToTempBlockIdsMap.get(sessionId);

    if (sessionTempBlockIds == null || sessionTempBlockIds.isEmpty()) {
      return Collections.emptyList();
    }
    List<TempBlockMeta> sessionTempBlocks = new ArrayList<>();
    for (long blockId : sessionTempBlockIds) {
      sessionTempBlocks.add(mBlockIdToTempBlockMap.get(blockId));
    }
    return sessionTempBlocks;
  }

  @Override
  public BlockStoreLocation toBlockStoreLocation() {
    return new BlockStoreLocation(mTier.getTierAlias(), mDirIndex, mDirMedium);
  }

  @Override
  public long getReservedBytes() {
    return mReservedBytes.get();
  }

  private void reclaimSpace(long size, boolean committed) {
    mAvailableBytes.getAndUpdate(oldAvailableBytes -> {
      long newAvailableBytes = oldAvailableBytes + size;
      checkState(mCapacityBytes >= newAvailableBytes,
              "Available bytes should always be less than total capacity bytes");
      return newAvailableBytes;
    });
    if (committed) {
      mCommittedBytes.addAndGet(-size);
    }
  }

  private void reserveSpace(long size, boolean committed) {
    mAvailableBytes.getAndUpdate(oldAvailableBytes -> {
      checkState(size <= oldAvailableBytes + getReservedBytes(),
              "Available bytes should always be non-negative");
      return oldAvailableBytes - size;
    });
    if (committed) {
      mCommittedBytes.addAndGet(size);
    }
  }
}
