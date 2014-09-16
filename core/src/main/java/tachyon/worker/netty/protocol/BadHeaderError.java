package tachyon.worker.netty.protocol;

public final class BadHeaderError extends StringError {
  private BadHeaderError(String message) {
    super(ResponseType.BadHeaderError, message);
  }

  public static BadHeaderError unknownType(int typeInt) {
    return new BadHeaderError("Unknown type for the given id " + typeInt);
  }

  public static BadHeaderError unsupportedVersion(long version, long currentVersion) {
    return new BadHeaderError("Unsupported version " + version + ", current version is "
        + currentVersion);
  }

  public static BadHeaderError merge(BadHeaderError error, BadHeaderError otherError) {
    return new BadHeaderError(error.getmMessage() + "; " + otherError.getmMessage());
  }
}
