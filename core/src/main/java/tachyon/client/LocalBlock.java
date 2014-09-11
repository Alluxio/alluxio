package tachyon.client;

import java.io.IOException;

final class LocalBlock implements Block {
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
    int bufferSize = (int) Math.min(mMaxBlockSize, 0x1000); // 4k
    LocalWritableBlockChannel local = new LocalWritableBlockChannel(mTachyonFS, mBlockId);
    BufferedWritableBlockChannel buffered = new BufferedWritableBlockChannel(bufferSize, local);
    BoundedWritableBlockChannel bounded = new BoundedWritableBlockChannel(mMaxBlockSize, buffered);
    return bounded;
  }
}
