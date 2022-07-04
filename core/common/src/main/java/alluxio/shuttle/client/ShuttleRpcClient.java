package alluxio.shuttle.client;

import alluxio.exception.ShuttleRpcException;
import alluxio.shuttle.handler.RpcCallback;
import alluxio.shuttle.handler.ShuttleRpcMessageProcessor;
import alluxio.shuttle.handler.ShuttleSyncRpcCallback;
import alluxio.shuttle.message.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.DefaultPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *  Underlying rpc client for msg transmit
 */
public class ShuttleRpcClient implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ShuttleRpcClient.class);

    private volatile boolean timeout;
    private final Channel channel;
    private final ShuttleRpcMessageProcessor handler;
    private final boolean server;

    public ShuttleRpcClient(boolean server, Channel channel, ShuttleRpcMessageProcessor handler) {
        this.channel = channel;
        this.handler = handler;
        this.server = server;
    }

    public void timeout() {
        timeout = true;
    }

    public boolean isActive() {
        return !timeout && (channel.isOpen() || channel.isActive());
    }

    public boolean isWritable() {
        return channel.isWritable();
    }

    public boolean isClosed() {
        return !channel.isOpen() && !channel.isActive();
    }

    public boolean isServer() {
        return server;
    }

    public String address() {
        if (channel != null) {
            return String.format("[%s -> %s]", channel.localAddress(), channel.remoteAddress());
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        close(null);
    }

    public void close(CountDownLatch latch) {
        channel.close().addListener((future -> {
            if (latch != null) {
                latch.countDown();
            }
            if (!future.isSuccess()) {
                LOG.warn("channel close fail", future.cause());
            }
        }));
    }

    public Channel getChannel() {
        return channel;
    }

    public void sendRpc(Message msg, RpcCallback callback) {
        handler.addRpc(msg.getReqId(), callback);
        channel.writeAndFlush(msg);
    }

    public void sendRpc(Message msg) {
        channel.writeAndFlush(msg);
    }

    public ChannelFuture writeAndFlush(Object msg) {
        return channel.writeAndFlush(msg);
    }

    public ChannelFuture write(Object msg) {
        return channel.write(msg);
    }

    public void setCallback(long reqId, RpcCallback callback) throws ShuttleRpcException {
        handler.setCallback(reqId, callback);
    }

    public void removeCallback(long reqId) throws ShuttleRpcException {
        handler.removeCallback(reqId);
    }

    public Message sendSync(Message msg, long timeoutMs) throws ShuttleRpcException {
        DefaultPromise<Message> promise = new DefaultPromise<>(channel.eventLoop());
        sendRpc(msg, new ShuttleSyncRpcCallback(promise));
        try {
            return promise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new ShuttleRpcException(e);
        }
    }

    public Message sendSync(Message msg) throws ShuttleRpcException {
        return sendSync(msg, Integer.MAX_VALUE);
    }

    public void execute(Runnable runnable) {
        channel.eventLoop().execute(runnable);
    }
}
