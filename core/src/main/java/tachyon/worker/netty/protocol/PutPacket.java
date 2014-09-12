package tachyon.worker.netty.protocol;

public final class PutPacket {
  private final PacketHeader header;
  private final byte[] data;

  public PutPacket(PacketHeader header, byte[] data) {
    this.header = header;
    this.data = data;
  }

  public PacketHeader getHeader() {
    return header;
  }

  public byte[] getData() {
    return data;
  }
}
