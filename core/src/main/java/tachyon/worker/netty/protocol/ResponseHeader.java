package tachyon.worker.netty.protocol;

public final class ResponseHeader {
  private final ResponseType type;

  public ResponseHeader(ResponseType type) {
    this.type = type;
  }

  public ResponseType getType() {
    return type;
  }
}
