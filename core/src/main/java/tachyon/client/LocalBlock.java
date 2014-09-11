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
    return new BoundedWritableBlockChannel(mMaxBlockSize, new LocalWritableBlockChannel(mTachyonFS, mBlockId));
  }
}
