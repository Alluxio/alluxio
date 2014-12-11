package tachyon.worker.netty.protocol;

public final class PutBlock {
  private final long mBlockId;

  public PutBlock(long blockId) {
    mBlockId = blockId;
  }

  public long getBlockId() {
    return mBlockId;
  }
}
