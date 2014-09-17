package tachyon.worker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.google.common.base.Optional;
import com.google.common.io.Closer;

import tachyon.worker.netty.protocol.RequestHeader;
import tachyon.worker.netty.protocol.RequestType;
import tachyon.worker.netty.protocol.ResponseType;

/**
 * This code is to verify the protocol between the different data implementations.
 */
public final class DataClient {
  private final String mHostname;
  private final int mPort;

  public DataClient(String hostname, int port) throws IOException {
    mHostname = hostname;
    mPort = port;
  }

  public GetBlock getBlock(long blockId, long offset, long length) throws IOException {
    final Socket socket = new Socket(mHostname, mPort);
    final Closer closer = Closer.create();
    try {
      final DataInputStream in = closer.register(new DataInputStream(socket.getInputStream()));
      final DataOutputStream out = closer.register(new DataOutputStream(socket.getOutputStream()));

      // Request Header
      writeHeader(out, RequestType.GetBlock);

      // Get Request
      out.writeLong(blockId);
      out.writeLong(offset);
      out.writeLong(length);

      return (GetBlock) response(out, in);
    } finally {
      closer.close();
      socket.close();
    }
  }

  private void writeHeader(DataOutputStream out, RequestType type) throws IOException {
    out.writeLong(RequestHeader.CURRENT_VERSION);
    out.writeInt(type.ordinal());
  }

  private Object response(DataOutputStream out, DataInputStream in) throws IOException {
    int typeCode = in.readInt();
    Optional<ResponseType> optType = ResponseType.valueOf(typeCode);
    if (optType.isPresent()) {
      ResponseType type = optType.get();
      switch (type) {
        case GetBlockResponse:
          return getBlockResponse(in);
        case InvalidBlockRange:
          throw new IOException("InvalidBlockRange");
        default:
          throw new AssertionError("Unsupported type: " + type);
      }
    } else {
      // bad response
      throw new AssertionError("Unknown type: " + typeCode);
    }
  }

  private GetBlock getBlockResponse(DataInputStream in) throws IOException {
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    if (blockId < 0) {
      // error response from NIO code base, make it a bad block
      throw new IOException("Invalid Block; " + Math.abs(blockId));
    }
    byte[] data = new byte[(int) length];
    in.read(data);
    return new GetBlock(blockId, offset, length, data);
  }

  public static final class GetBlock {
    private final long blockId;
    private final long offset;
    private final long length;
    private final byte[] data;

    public GetBlock(long blockId, long offset, long length, byte[] data) {
      this.blockId = blockId;
      this.offset = offset;
      this.length = length;
      this.data = data;
    }

    public long getBlockId() {
      return blockId;
    }

    public long getOffset() {
      return offset;
    }

    public long getLength() {
      return length;
    }

    public byte[] getData() {
      return data;
    }
  }
}
