package tachyon.client;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * TachyonByteBuffer is a wrapper on Java ByteBuffer plus some information needed by Tachyon.
 */
public class TachyonByteBuffer implements Closeable {
  // ByteBuffer contains data.
  public final ByteBuffer DATA;

  private final long mBlockId;

  private final int mBlockLockId;

  private final TachyonFS mTachyonFS;

  private boolean mClosed = false;

  /**
   * @param tfs
   *          the Tachyon file system
   * @param buf
   *          the ByteBuffer wrapped on
   * @param blockId
   *          the id of the block
   * @param blockLockId
   *          the id of the block's lock
   */
  TachyonByteBuffer(TachyonFS tfs, ByteBuffer buf, long blockId, int blockLockId) {
    DATA = buf;
    mBlockId = blockId;
    mBlockLockId = blockLockId;
    mTachyonFS = tfs;
  }

  /**
   * Close the TachyonByteBuffer, here it is synchronized
   * 
   * @throws IOException
   */
  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }

    mClosed = true;
    if (mBlockLockId >= 0) {
      mTachyonFS.unlockBlock(mBlockId, mBlockLockId);
    }
  }
}
