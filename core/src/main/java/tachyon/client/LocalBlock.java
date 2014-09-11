package tachyon.client;

import java.io.IOException;

final class LocalBlock implements Block {
  private static final long DEFAULT_BUFFER_SIZE = 4096; // 4k

  private final TachyonFS mTachyonFS;
  private final long mBlockId;
  private final long mMaxBlockSize;

  LocalBlock(TachyonFS tachyonFS, long blockId, long maxBlockSize) {
    this.mTachyonFS = tachyonFS;
    this.mBlockId = blockId;
    this.mMaxBlockSize = maxBlockSize;
  }

  @Override
  public WritableBlockChannel write() throws IOException {
    int bufferSize = (int) Math.min(mMaxBlockSize, DEFAULT_BUFFER_SIZE);
    LocalWritableBlockChannel local = new LocalWritableBlockChannel(mTachyonFS, mBlockId);
    BufferedWritableBlockChannel buffered = new BufferedWritableBlockChannel(bufferSize, local);
    BoundedWritableBlockChannel bounded = new BoundedWritableBlockChannel(mMaxBlockSize, buffered);
    return bounded;
  }
}
