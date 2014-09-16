package tachyon.worker.netty.protocol;

public enum ResponseType {
  GetBlockResponse,
  PutBlockSuccess,
  BlockNotFound,
  UnknownError,
  BadHeaderError,
  InvalidBlockId,
  InvalidBlockRange
}
