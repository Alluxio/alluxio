package alluxio.worker.block.meta;

/**
 * A view of {@link StorageDir} to provide more limited access.
 */
public interface DirView {
  /**
   * Creates a {@link TempBlockMeta} given sessionId, blockId, and initialBlockSize.
   *
   * @param sessionId of the owning session
   * @param blockId of the new block
   * @param initialBlockSize of the new block
   * @return a new {@link TempBlockMeta} under the underlying directory
   */
  TempBlockMeta createTempBlockMeta(long sessionId, long blockId, long initialBlockSize);
}
