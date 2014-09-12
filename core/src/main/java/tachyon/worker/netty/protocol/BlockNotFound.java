package tachyon.worker.netty.protocol;

public final class BlockNotFound {
  private final long blockId;

  public BlockNotFound(long blockId) {
    this.blockId = blockId;
  }

  public long getBlockId() {
    return blockId;
  }
}
