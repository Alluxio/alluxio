package alluxio.shuttle.handler;

import alluxio.exception.ShuttleRpcException;
import alluxio.shuttle.client.ShuttleRpcClient;
import alluxio.shuttle.executor.GroupExecutor;
import alluxio.shuttle.message.Message;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShuttleRpcMessageProcessor {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected GroupExecutor executor;
    protected ShuttleRpcClient client;
    private final Map<Long, RpcCallback> flightingRpc;

    public ShuttleRpcMessageProcessor() {
        flightingRpc = new ConcurrentHashMap<>();
    }

    public synchronized void setClientAndExecutor(GroupExecutor executor, ShuttleRpcClient client) {
        this.client = client;
        this.executor = executor;
        this.init();
    }

    public void init() {
    }

    public void setClientAndExecutor(ShuttleRpcClient client) {
        setClientAndExecutor(null, client);
    }

    public long getAssignId(Message msg) {
        return msg.getReqId();
    }

    public void handle(ChannelHandlerContext ctx, Message msg) throws Exception {
        if (executor == null) {
            // If the executor is null, the handler does not release the buffer because there is no way
            // to know when the data processing will end.
            call(msg);
        } else {
            executor.execute(getAssignId(msg), () -> {
                boolean isRelease = false;
                try {
                    isRelease = call(msg);
                } catch (Exception e) {
                    ctx.fireExceptionCaught(e);
                } finally {
                    if (!isRelease) {
                        msg.releaseBody();
                    }
                }
            });
        }
    }


    public boolean call(Message msg) {
        RpcCallback callback = getCallback(msg);
        if (callback == null) {
            LOG.warn("Ignoring response for reqId {} from {} since it is complete", msg.getReqId(), client.address());
            msg.releaseBody();
            return true;
        }

        if (!msg.isError()) {
            callback.onSuccess(msg);
            return false;
        } else {
            IOException ioException = new IOException(msg.getNettyBody().toString(Charset.defaultCharset()));
            callback.onFailure(msg.getReqId(), new IOException(ioException));
            msg.releaseBody();
            return true;
        }
    }

    public int getFlightingRpcCount() {
        return flightingRpc.size();
    }

    public boolean hasFlightingRpc() {
        return getFlightingRpcCount() > 0;
    }


    public void channelActive(ChannelHandlerContext ctx) {
        // pass
    }

    public void channelInactive(ChannelHandlerContext ctx) throws IOException {
        if (hasFlightingRpc()) {
            LOG.error("Still have {} requests not completed when connection from {} is closed",
                    getFlightingRpcCount(), client.address());
            failFlightingRpc(new IOException("Connection " + client.address() + " closed"));
        }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        if (hasFlightingRpc()) {
            LOG.error("Still have {} requests not completed when connection from {} is closed",
                    getFlightingRpcCount(), client.address());
            failFlightingRpc(e);
        }
    }


    public void channelTimeout(ChannelHandlerContext ctx) {
    }

    public void failFlightingRpc(Throwable cause) {
        for (Map.Entry<Long, RpcCallback> entry : flightingRpc.entrySet()) {
            try {
                entry.getValue().onFailure(entry.getKey(), cause);
            } catch (Exception e) {
                LOG.error("rpc.onFailure throws exception", e);
            }
        }

        flightingRpc.clear();
    }

    public void addRpc(long reqId, RpcCallback callback) {
        flightingRpc.put(reqId, callback);
    }

    private RpcCallback getCallback(Message msg) {
        RpcCallback callback;
        if (msg.isLastReq())  {
            callback = flightingRpc.remove(msg.getReqId());
        } else {
            callback = flightingRpc.get(msg.getReqId());
            if (callback instanceof ShuttleSyncRpcCallback) {
                flightingRpc.remove(msg.getReqId());
            }
        }
        return callback;
    }



    public synchronized void setCallback(long reqId, RpcCallback callback) throws ShuttleRpcException {
        if (flightingRpc.containsKey(reqId)) {
            LOG.warn("The reqId:{} callback already exists", reqId);
            throw new ShuttleRpcException("The reqId " + reqId +
                    " callback function already exists, it is not allowed to set it repeatedly");
        } else {
            flightingRpc.put(reqId, callback);
        }
    }

    public void removeCallback(long reqId) throws ShuttleRpcException {
        if (!flightingRpc.containsKey(reqId)) {
            LOG.warn("The reqId:{} not exists in flighting rpc", reqId);
            throw new ShuttleRpcException("The reqId " + reqId +
                    " callback function does not exist");
        } else {
            flightingRpc.remove(reqId);
        }
    }

    public void onSuccess(Message msg) {
        getCallback(msg).onSuccess(msg);
    }

    public void onFailure(Message msg, Throwable cause) {
        getCallback(msg).onFailure(msg.getReqId(), cause);
    }

}
