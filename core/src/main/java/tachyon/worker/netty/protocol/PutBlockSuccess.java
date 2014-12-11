package tachyon.worker.netty.protocol;

public final class PutBlockSuccess {
  private final long mBlockId;

  public PutBlockSuccess(long blockId) {
    mBlockId = blockId;
  }

  public long getBlockId() {
    return mBlockId;
  }
}
