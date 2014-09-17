package tachyon.worker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.google.common.base.Optional;
import com.google.common.io.Closer;

import tachyon.worker.netty.protocol.RequestHeader;
import tachyon.worker.netty.protocol.RequestType;
import tachyon.worker.netty.protocol.ResponseType;

/**
 * Client for the data protocol.
 */
public final class DataClient {
  private static final long SKIP_INPUT = -1;

  private final String mHostname;
  private final int mPort;

  public DataClient(String hostname, int port) {
    mHostname = hostname;
    mPort = port;
  }

  public DataClient(InetSocketAddress address) {
    this(address.getHostName(), address.getPort());
  }

  public GetBlock getBlock(long blockId) throws IOException {
    return getBlock(blockId, SKIP_INPUT, SKIP_INPUT);
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

      return getBlockResponse(in);
    } finally {
      closer.close();
      socket.close();
    }
  }

  private void writeHeader(DataOutputStream out, RequestType type) throws IOException {
    out.writeLong(RequestHeader.CURRENT_VERSION);
    out.writeInt(type.ordinal());
  }

  private GetBlock getBlockResponse(DataInputStream in) throws IOException {
    int typeCode = in.readInt();
    Optional<ResponseType> optType = ResponseType.valueOf(typeCode);
    if (optType.isPresent()) {
      ResponseType type = optType.get();
      switch (type) {
        case GetBlockResponse:
          return parseGetBlock(in);
        case InvalidBlockRange:
          throw new IOException("InvalidBlockRange");
        default:
          throw new AssertionError("Unsupported type: " + type);
      }
    } else {
      // bad getBlockResponse
      throw new AssertionError("Unknown type: " + typeCode);
    }
  }

  private GetBlock parseGetBlock(DataInputStream in) throws IOException {
    long blockId = in.readLong();
    long offset = in.readLong();
    long length = in.readLong();
    if (blockId < 0) {
      // NIO server uses -blockId to denote errors
      // so make it a IO to match netty
      throw new IOException("Invalid Block; " + Math.abs(blockId));
    }
    //TODO can be unsafe, but the main caller of this API does pagination
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
