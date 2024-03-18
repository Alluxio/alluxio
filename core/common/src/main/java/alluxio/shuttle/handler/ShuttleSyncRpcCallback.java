package alluxio.shuttle.handler;

import alluxio.shuttle.message.Message;
import io.netty.util.concurrent.DefaultPromise;

public class ShuttleSyncRpcCallback implements RpcCallback{
    private final DefaultPromise<Message> promise;

    public ShuttleSyncRpcCallback(DefaultPromise<Message> promise) {
        this.promise = promise;
    }

    @Override
    public void onSuccess(Message message) {
        promise.setSuccess(message);
    }

    @Override
    public void onFailure(long reqId, Throwable cause) {
        promise.setFailure(cause);
    }
}
