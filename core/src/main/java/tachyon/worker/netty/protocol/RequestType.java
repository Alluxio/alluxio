package tachyon.worker.netty.protocol;

import com.google.common.base.Optional;

public enum RequestType {
  GetBlock,
  PutBlock;

  public static Optional<RequestType> valueOf(int ordinal) {
    for (RequestType type : RequestType.values()) {
      if (type.ordinal() == ordinal) {
        return Optional.of(type);
      }
    }
    return Optional.absent();
  }
}
