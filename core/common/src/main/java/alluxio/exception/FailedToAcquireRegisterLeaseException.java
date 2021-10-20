package alluxio.exception;

public class FailedToAcquireRegisterLeaseException extends AlluxioException {
    private static final long serialVersionUID = 8148784998226149671L;

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public FailedToAcquireRegisterLeaseException(String message) {
      super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public FailedToAcquireRegisterLeaseException(String message, Throwable cause) {
      super(message, cause);
    }

    /**
     * Constructs a new exception with the specified exception message and multiple parameters.
     *
     * @param message the exception message
     * @param params the parameters
     */
    public FailedToAcquireRegisterLeaseException(ExceptionMessage message, Object... params) {
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
    public FailedToAcquireRegisterLeaseException(ExceptionMessage message, Throwable cause, Object... params) {
      this(message.getMessage(params), cause);
    }
}
