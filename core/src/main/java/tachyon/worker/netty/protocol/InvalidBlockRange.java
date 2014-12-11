package tachyon.worker.netty.protocol;

public final class InvalidBlockRange extends BlockError {
  private final long mOffset;
  private final long mLength;

  public InvalidBlockRange(long blockId, long offset, long length) {
    super(ResponseType.InvalidBlockRange, blockId);
    mOffset = offset;
    mLength = length;
  }

  public long getmOffset() {
    return mOffset;
  }

  public long getmLength() {
    return mLength;
  }
}
