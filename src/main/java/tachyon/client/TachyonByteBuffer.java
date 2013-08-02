package tachyon.client;

import java.nio.ByteBuffer;

/**
 * TachyonByteBuffer is a wrapper on Java ByteBuffer plus some information needed by Tachyon.
 */
public class TachyonByteBuffer {
  // ByteBuffer contains data.
  public final ByteBuffer DATA;

  private final long BLOCK_ID;  

  private final int BLOCK_LOCK_ID;

  private final TachyonFS TFS;

  private boolean mClosed = false;

  TachyonByteBuffer(TachyonFS tfs, ByteBuffer buf, long blockId, int blockLockId) {
    DATA = buf;
    BLOCK_ID = blockId;
    BLOCK_LOCK_ID = blockLockId;
    TFS = tfs;
  }

  public synchronized void close() {
    if (mClosed) {
      return;
    }

    mClosed = true;
    if (BLOCK_LOCK_ID >= 0) {
      TFS.unlockBlock(BLOCK_ID, BLOCK_LOCK_ID);
    }
  }
}
