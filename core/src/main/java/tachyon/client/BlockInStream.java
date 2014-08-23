package tachyon.client;

import java.io.IOException;

/**
 * <code>InputStream</code> interface implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by the
 * client code.
 */
public abstract class BlockInStream extends InStream {
  /**
   * Get a new BlockInStream of the given block without under file system configuration. The block
   * is decided by the tachyonFile and blockIndex
   * 
   * @param tachyonFile
   *          the file the block belongs to
   * @param readType
   *          the InStream's read type
   * @param blockIndex
   *          the index of the block in the tachyonFile
   * @return A new LocalBlockInStream or RemoteBlockInStream
   * @throws IOException
   */
  public static BlockInStream get(TachyonFile tachyonFile, ReadType readType, int blockIndex)
      throws IOException {
    return get(tachyonFile, readType, blockIndex, null);
  }

  /**
   * Get a new BlockInStream of the given block with the under file system configuration. The block
   * is decided by the tachyonFile and blockIndex
   * 
   * @param tachyonFile
   *          the file the block belongs to
   * @param readType
   *          the InStream's read type
   * @param blockIndex
   *          the index of the block in the tachyonFile
   * @param ufsConf
   *          the under file system configuration
   * @return A new LocalBlockInStream or RemoteBlockInStream
   * @throws IOException
   */
  public static BlockInStream get(TachyonFile tachyonFile, ReadType readType, int blockIndex,
      Object ufsConf) throws IOException {
    TachyonByteBuffer buf = tachyonFile.readLocalByteBuffer(blockIndex);
    if (buf != null) {
      return new LocalBlockInStream(tachyonFile, readType, blockIndex, buf);
    }

    return new RemoteBlockInStream(tachyonFile, readType, blockIndex, ufsConf);
  }

  protected final int mBlockIndex;

  protected boolean mClosed = false;

  /**
   * @param file
   * @param readType
   * @param blockIndex
   * @throws IOException
   */
  BlockInStream(TachyonFile file, ReadType readType, int blockIndex) throws IOException {
    super(file, readType);
    mBlockIndex = blockIndex;
  }
}
