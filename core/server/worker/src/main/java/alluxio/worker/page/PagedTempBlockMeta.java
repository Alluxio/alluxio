package alluxio.worker.page;

import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.DefaultBlockMeta;
import alluxio.worker.block.meta.DefaultTempBlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;

/**
 * Represents the metadata of an uncommitted block in paged local storage.
 */
public class PagedTempBlockMeta extends DefaultTempBlockMeta {

  /**
   * Creates a new instance of {@link DefaultTempBlockMeta}.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @param initialBlockSize initial size of this block in bytes
   * @param dir {@link StorageDir} of this temp block belonging to
   */
  public PagedTempBlockMeta(long sessionId, long blockId, long initialBlockSize, StorageDir dir) {
    super(sessionId, blockId, initialBlockSize, dir);
  }

  @Override
  public String getCommitPath() {
    return "";
  }

  @Override
  public BlockStoreLocation getBlockLocation() {
    return BlockStoreLocation.anyTier();
  }
}
