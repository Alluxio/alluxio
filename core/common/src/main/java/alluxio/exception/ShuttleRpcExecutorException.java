package alluxio.exception;

/**
 *
 */
public class ShuttleRpcExecutorException extends AlluxioException {
    /**
     * Constructs a new exception with the specified detail cause.
     *
     * @param cause the cause
     */
    protected ShuttleRpcExecutorException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public ShuttleRpcExecutorException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public ShuttleRpcExecutorException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the input exception.
     *
     * @param e the exception
     */
    public ShuttleRpcExecutorException(Exception e) {
        super(e);
    }
}
