package alluxio.shuttle.handler;


import alluxio.shuttle.message.Message;

public interface RpcCallback {
    /**
     * Request success callback
     */
    void onSuccess(Message message);

    /**
     * Request failure callback
     */
    void onFailure(long reqId, Throwable cause);

    /**
     * Channel exception callback, such as connection close, timeout, etc.
     */
    default void onChannelError(Throwable cause) {};
}
