package alluxio.worker.block;

import alluxio.worker.block.meta.DirView;
import alluxio.worker.block.meta.TempBlockMeta;

/**
 * Factory for TempBlockMeta.
 */
public interface TempBlockMetaFactory {

  /**
   * @param sessionId
   * @param blockId
   * @param initialBlockSize
   * @param dirView
   * @return TempBlockMeta
   */
  TempBlockMeta createTempBlockMeta(long sessionId, long blockId, long initialBlockSize,
      DirView dirView);
}
