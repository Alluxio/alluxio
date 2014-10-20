package tachyon.worker.netty.protocol;

public final class PutPacket {
  private final PacketHeader mHeader;
  private final byte[] mData;

  public PutPacket(PacketHeader header, byte[] data) {
    mHeader = header;
    mData = data;
  }

  public PacketHeader getHeader() {
    return mHeader;
  }

  public byte[] getData() {
    return mData;
  }
}
