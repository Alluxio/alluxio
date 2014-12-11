package tachyon.worker.netty.protocol;

import com.google.common.base.Optional;

public enum ResponseType {
  GetBlockResponse,
  PutBlockSuccess,
  BlockNotFound,
  UnknownError,
  BadHeaderError,
  InvalidBlockId,
  InvalidBlockRange;

  public static Optional<ResponseType> valueOf(int ordinal) {
    for (ResponseType type : ResponseType.values()) {
      if (type.ordinal() == ordinal) {
        return Optional.of(type);
      }
    }
    return Optional.absent();
  }
}
