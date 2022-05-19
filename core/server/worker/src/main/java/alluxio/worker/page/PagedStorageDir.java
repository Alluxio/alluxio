package alluxio.worker.page;

import alluxio.client.file.cache.PageInfo;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.resource.LockResource;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

/**
 *
 */
public class PagedStorageDir {

  /** The number of logical bytes used. */
  private final AtomicLong mBytes = new AtomicLong(0);
  /** The number of pages stored. */
  private final AtomicLong mPages = new AtomicLong(0);
  private final ReentrantReadWriteLock mBlockPageMapLock = new ReentrantReadWriteLock();
  @GuardedBy("mBlockPageMapLock")
  private final Map<Long, Set<Long>> mBlockPageMap = new HashMap<>();

  public long getCapacityBytes() {
    return 0;
  }

  public long getAvailableBytes() {
    return 0;
  }

  public long getCommittedBytes() {
    return 0;
  }

  public String getDirPath() {
    return null;
  }

  public String getDirMedium() {
    return null;
  }

  public List<Long> getBlockIds() {
    return null;
  }

  public List<BlockMeta> getBlocks() {
    return null;
  }

  public boolean hasBlockMeta(long blockId) {
    return false;
  }

  public boolean hasTempBlockMeta(long blockId) {
    return false;
  }

  public Optional<BlockMeta> getBlockMeta(long blockId) {
    return Optional.empty();
  }

  public TempBlockMeta getTempBlockMeta(long blockId) {
    return null;
  }

  public void addPage(PageInfo pageInfo) {

  }

  /**
   *
   * @param pageInfo
   * @throws WorkerOutOfSpaceException
   * @throws BlockAlreadyExistsException
   */
  public void addTempPage(PageInfo pageInfo)
      throws WorkerOutOfSpaceException, BlockAlreadyExistsException {
    long blockId = Long.parseLong(pageInfo.getPageId().getFileId());
    try (LockResource lock = new LockResource(mBlockPageMapLock.writeLock())) {
      mBlockPageMap
          .computeIfAbsent(blockId, k -> new HashSet<>())
          .add(pageInfo.getPageId().getPageIndex());
    }
    mBytes.addAndGet(pageInfo.getPageSize());
    mPages.incrementAndGet();
  }

  @Override
  public void removeBlockMeta(BlockMeta blockMeta) {

  }

  @Override
  public void removeTempBlockMeta(TempBlockMeta tempBlockMeta) throws BlockDoesNotExistException {

  }

  @Override
  public void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize)
      throws InvalidWorkerStateException {

  }

  @Override
  public void cleanupSessionTempBlocks(long sessionId, List<Long> tempBlockIds) {

  }

  @Override
  public List<TempBlockMeta> getSessionTempBlocks(long sessionId) {
    return null;
  }

  @Override
  public BlockStoreLocation toBlockStoreLocation() {
    return null;
  }

  @Override
  public long getReservedBytes() {
    return 0;
  }
}
