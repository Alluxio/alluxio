package alluxio.exception;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a collection of exceptions.
 */
public class AggregateException extends IOException {
  private final Collection<Exception> mExceptions;

  /**
   * Creates a new instance of {@link AggregateException}.
   *
   * @param exceptions the nested exceptions
   */
  public AggregateException(Collection<Exception> exceptions) {
    mExceptions = exceptions;
  }

  @Override
  public String getMessage() {
    return toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (Exception e : mExceptions) {
      sb.append("Exception #").append(++i).append(":\n");
      sb.append(e.toString()).append("\n");
    }
    return sb.toString();
  }
}
