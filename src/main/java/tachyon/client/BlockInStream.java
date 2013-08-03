package tachyon.client;

import java.io.IOException;

/**
 * <code>InputStream</code> interface implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by
 * the client code.
 * 
 * TODO(Haoyuan) Further separate this into RemoteBlockInStream and LocalBlockInStream.
 */
public abstract class BlockInStream extends InStream {
  protected final int BLOCK_INDEX;
  protected boolean mClosed = false;

  /**
   * @param file
   * @param readType
   * @param blockIndex
   * @throws IOException
   */
  BlockInStream(TachyonFile file, ReadType readType, int blockIndex) throws IOException {
    super(file, readType);
    BLOCK_INDEX = blockIndex;
  }

  public static BlockInStream get(TachyonFile tachyonFile, ReadType readType, int blockIndex)
      throws IOException {

    TachyonByteBuffer buf = tachyonFile.readLocalByteBuffer(blockIndex);
    if (buf != null) {
      return new LocalBlockInStream(tachyonFile, readType, blockIndex, buf);
    }

    return new RemoteBlockInStream(tachyonFile, readType, blockIndex);
  }
}
