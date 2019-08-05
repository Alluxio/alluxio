package alluxio.exception.metaservice;

import alluxio.exception.AlluxioException;

/**
 * The exeception thrown when the object in Alluxio meta serivice is invalid. For example,
 * getTable but there isn't any table under selected database.
 */
public class NoSuchObjectException extends AlluxioException {
    private static final long serialVersionUID = 3218270155094188299L;

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public NoSuchObjectException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified cause.
     *
     * @param cause the cause of the exception
     */
    public NoSuchObjectException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    NoSuchObjectException(String message, Throwable cause) {
        super(message, cause);
    }
}
