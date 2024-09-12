package alluxio.exception;

/**
 * Exception thrown when error occurs using ShuttleRpc protocol
 */
public class ShuttleRpcException extends AlluxioException {
    private static final long serialVersionUID = 305829608542844656L;

    /**
     * Constructs a new exception with the specified detail cause.
     *
     * @param cause the cause
     */
    protected ShuttleRpcException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public ShuttleRpcException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public ShuttleRpcException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the input exception.
     *
     * @param e the exception
     */
    public ShuttleRpcException(Exception e) {
        super(e);
    }
}
