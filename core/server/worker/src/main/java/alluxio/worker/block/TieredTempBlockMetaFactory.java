package alluxio.worker.block;

import alluxio.worker.block.meta.DirView;
import alluxio.worker.block.meta.TempBlockMeta;

/**
 * Factory of TempBlockMeta.
 */
public class TieredTempBlockMetaFactory implements TempBlockMetaFactory {

  @Override
  public TempBlockMeta createTempBlockMeta(long sessionId, long blockId, long initialBlockSize,
      DirView dirView) {
    // TODO(carson): Add tempBlock to corresponding storageDir and remove the use of
    // StorageDirView.createTempBlockMeta.
    return dirView.createTempBlockMeta(sessionId, blockId, initialBlockSize);
  }
}
