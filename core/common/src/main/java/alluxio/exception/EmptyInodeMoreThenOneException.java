package alluxio.exception;

import alluxio.AlluxioURI;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class EmptyInodeMoreThenOneException extends AlluxioException {
  private static final long serialVersionUID = -3162552183420120201L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public EmptyInodeMoreThenOneException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public EmptyInodeMoreThenOneException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new exception with the specified exception message and multiple parameters.
   *
   * @param message the exception message
   * @param params the parameters
   */
  public EmptyInodeMoreThenOneException(ExceptionMessage message, Object... params) {
    this(message.getMessage(params));
  }

  /**
   * Constructs a new exception with the specified exception message, the cause and multiple
   * parameters.
   *
   * @param message the exception message
   * @param cause the cause
   * @param params the parameters
   */
  public EmptyInodeMoreThenOneException(ExceptionMessage message, Throwable cause, Object... params) {
    this(message.getMessage(params), cause);
  }

  /**
   * Constructs a new exception stating that the given path does not exist.
   *
   */
  public EmptyInodeMoreThenOneException() {
    this(ExceptionMessage.EMPTY_INODE_MORE_THAN_ONE.getMessage());
  }
}
