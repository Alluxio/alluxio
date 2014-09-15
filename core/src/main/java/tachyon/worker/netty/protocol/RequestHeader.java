package tachyon.worker.netty.protocol;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public final class RequestHeader {
  public static final long CURRENT_VERSION = 2;
  public static final long HEADER_SIZE = Longs.BYTES + Ints.BYTES;

  private final long version;
  private final RequestType type;

  private RequestHeader(long version, RequestType type) {
    this.version = version;
    this.type = type;
  }

  public long getVersion() {
    return version;
  }

  public RequestType getType() {
    return type;
  }
}
