package tachyon.worker.netty;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import tachyon.worker.BlocksLocker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

public final class NettyDataServer implements Closeable {
  private final EventLoopGroup BOSS_GROUP = new NioEventLoopGroup();
  private final EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();
//  private final EventExecutorGroup SYNC_GROUP = new DefaultEventExecutorGroup(16);
  private final ChannelFuture CHANNEL_FUTURE;

  public NettyDataServer(final SocketAddress address, final BlocksLocker locker)
      throws InterruptedException {
    ChannelInitializer<SocketChannel> childHandler = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("blockRequestDecoder", new BlockRequest.Decoder());
        pipeline.addLast("blockRequestEncoder", new BlockResponse.Encoder());
        pipeline.addLast("nioChunckedWriter", new ChunkedWriteHandler());
        // TODO move out of worker group
//        pipeline.addLast(SYNC_GROUP, "dataServerHandler", new DataServerHandler(locker));
        pipeline.addLast("dataServerHandler", new DataServerHandler(locker));
      }
    };

    ServerBootstrap bootstrap =
        new ServerBootstrap()
            .group(BOSS_GROUP, WORKER_GROUP)
            // TODO look into EpollSocketChannel if faster than FileRegion
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.DEBUG))
            .childHandler(childHandler)
            .option(ChannelOption.SO_BACKLOG, 1024)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

    this.CHANNEL_FUTURE = bootstrap.bind(address).sync();
  }

  @Override
  public void close() throws IOException {
    BOSS_GROUP.shutdownGracefully();
    WORKER_GROUP.shutdownGracefully();
  }

  /**
   * Gets the port listening on.
   */
  public int getPort() {
    // according to the docs, a InetSocketAddress is returned and the user must down-cast
    return ((InetSocketAddress) CHANNEL_FUTURE.channel().localAddress()).getPort();
  }

  public boolean isClosed() {
    return BOSS_GROUP.isShutdown();
  }
}
