package tachyon.worker.netty.protocol;

public final class InvalidBlockId extends BlockError {
  public InvalidBlockId(long blockId) {
    super(ResponseType.InvalidBlockId, blockId);
  }
}
