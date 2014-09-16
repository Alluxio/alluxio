package tachyon.worker.netty.protocol;

public final class UnknownError extends StringError {
  public UnknownError(String message) {
    super(ResponseType.UnknownError, message);
  }

  public UnknownError(Throwable throwable) {
    this(throwable.getMessage());
  }
}
