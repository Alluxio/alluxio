package tachyon.worker.netty.protocol;

public final class PutBlock {
  private final long blockId;

  public PutBlock(long blockId) {
    this.blockId = blockId;
  }

  public long getBlockId() {
    return blockId;
  }
}
