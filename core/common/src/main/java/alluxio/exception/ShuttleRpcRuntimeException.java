package alluxio.exception;

public class ShuttleRpcRuntimeException extends RuntimeException{
    public ShuttleRpcRuntimeException() {
    }

    public ShuttleRpcRuntimeException(String message) {
        super(message);
    }

    public ShuttleRpcRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ShuttleRpcRuntimeException(Throwable cause) {
        super(cause);
    }

    public ShuttleRpcRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
