package alluxio.shuttle.client;

import alluxio.shuttle.handler.ShuttleRpcMessageProcessor;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.lang3.SystemUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Base info for Shuttle rpc protocol client
 */
public class ShuttleClientInfo {
    private final int timeoutMs;
    private final int ioThreads;
    private final int numConnections;
    private final boolean _closeIdleConnections;
    private final Map<ChannelOption<?>, Object> channelOptions;
    private final Supplier<ShuttleRpcMessageProcessor> msgProcessorSupplier;

    public ShuttleClientInfo(
            int timeoutMs,
            int ioThreads,
            int numConnections,
            boolean closeIdleConnections,
            Map<ChannelOption<?>, Object> channelOptions,
            Supplier<ShuttleRpcMessageProcessor> msgProcessorSupplier) {
        this.timeoutMs = timeoutMs;
        this.ioThreads = ioThreads;
        this.numConnections = numConnections;
        this._closeIdleConnections = closeIdleConnections;
        this.channelOptions = channelOptions;
        this.msgProcessorSupplier = msgProcessorSupplier;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public int getIoThreads() {
        return ioThreads;
    }

    public int getNumConnections() {
        return numConnections;
    }

    public Map<ChannelOption<?>, Object> getChannelOptions() {
        return channelOptions;
    }

    public Supplier<ShuttleRpcMessageProcessor> getMsgProcessorSupplier() {
        return msgProcessorSupplier;
    }

    public Class<? extends SocketChannel> getChannelClass() {
        if (SystemUtils.IS_OS_LINUX) {
            return EpollSocketChannel.class;
        } else {
            return NioSocketChannel.class;
        }
    }


    public boolean useEpoll() {
        return SystemUtils.IS_OS_LINUX;
    }

    public boolean closeIdleConnections() {
        return _closeIdleConnections;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("ClientInfo{timeoutMs=%d, ioThreads=%d, channelClass=%s, numConnections=%d}",
                timeoutMs,
                ioThreads,
                ioThreads,
                getChannelClass().getSimpleName(),
                numConnections);
    }

    public static class Builder {
        private int timeoutMs;
        private int ioThreads;
        private boolean closeIdleConnections;
        private int numConnections;
        private final Map<ChannelOption<?>, Object> channelOptions = new HashMap<>();
        private Supplier<ShuttleRpcMessageProcessor> msgProcessorSupplier;

        public Builder() {
            timeoutMs = 120_000;
            ioThreads = 16;
            numConnections = 1;
            closeIdleConnections = true;

            // Set system default parameters
            channelOptions.put(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            channelOptions.put(ChannelOption.TCP_NODELAY, true);
            channelOptions.put(ChannelOption.SO_KEEPALIVE, true);
            channelOptions.put(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMs);
        }

        public Builder setTimeoutMs(int timeoutMs) {
            this.timeoutMs = timeoutMs;
            channelOptions.put(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMs);
            return this;
        }

        public Builder setIoThreads(int ioThreads) {
            this.ioThreads = ioThreads;
            return this;
        }

        public Builder setNumConnections(int numConnections) {
            this.numConnections = numConnections;
            return this;
        }

        public Builder setHandlerSupplier(Supplier<ShuttleRpcMessageProcessor> msgProcessorSupplier) {
            this.msgProcessorSupplier = msgProcessorSupplier;
            return this;
        }

        public Builder addChannelOption(ChannelOption<?> key, Object value) {
            channelOptions.put(key, value);
            return this;
        }

        public Builder setCloseIdleConnections(boolean closeIdleConnections) {
            this.closeIdleConnections = closeIdleConnections;
            return this;
        }

        public ShuttleClientInfo build() {
            assert  msgProcessorSupplier != null;
            return new ShuttleClientInfo(
                    timeoutMs,
                    ioThreads,
                    numConnections,
                    closeIdleConnections,
                    channelOptions,
                    msgProcessorSupplier);
        }
    }
}
