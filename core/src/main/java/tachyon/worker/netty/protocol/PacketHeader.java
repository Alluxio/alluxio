package tachyon.worker.netty.protocol;

public final class PacketHeader {
  private final long blockOffset;
  private final long sequenceNumber;
  private final long length;
  private final boolean last;

  private PacketHeader(long blockOffset, long sequenceNumber, long length, boolean last) {
    this.blockOffset = blockOffset;
    this.sequenceNumber = sequenceNumber;
    this.length = length;
    this.last = last;
  }
}
