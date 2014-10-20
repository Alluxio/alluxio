package tachyon.worker.netty.protocol;

public final class PacketHeader {
  private final long mBlockOffset;
  private final long mSequenceNumber;
  private final long mLength;
  private final boolean mLast;

  private PacketHeader(long blockOffset, long sequenceNumber, long length, boolean last) {
    mBlockOffset = blockOffset;
    mSequenceNumber = sequenceNumber;
    mLength = length;
    mLast = last;
  }
}
