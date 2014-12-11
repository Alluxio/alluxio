package tachyon.worker.netty.protocol;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public final class RequestHeader {
  public static final long CURRENT_VERSION = 2;
  public static final long HEADER_SIZE = Longs.BYTES + Ints.BYTES;

  private final long mVersion;
  private final RequestType mType;

  private RequestHeader(long version, RequestType type) {
    mVersion = version;
    mType = type;
  }

  public long getVersion() {
    return mVersion;
  }

  public RequestType getType() {
    return mType;
  }
}
