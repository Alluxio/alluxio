package tachyon.worker.netty.protocol;

public final class PutBlockSuccess {
  private final long blockId;

  public PutBlockSuccess(long blockId) {
    this.blockId = blockId;
  }

  public long getBlockId() {
    return blockId;
  }
}
