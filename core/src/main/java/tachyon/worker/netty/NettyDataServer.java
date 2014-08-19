package tachyon.worker.netty;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import tachyon.worker.BlocksLocker;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public final class NettyDataServer implements Closeable {
  private final EventLoopGroup BOSS_GROUP = new NioEventLoopGroup();
  private final EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();
  private final ChannelFuture CHANNEL_FUTURE;

  public NettyDataServer(final SocketAddress address, final BlocksLocker locker)
      throws InterruptedException {
    ChannelInitializer<SocketChannel> childHandler = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new BlockRequest.Decoder(), new BlockResponse.Encoder(),
            new ChunkedWriteHandler(),
            new DataServerHandler(locker));
      }
    };

    ServerBootstrap bootstrap =
        new ServerBootstrap()
            .group(BOSS_GROUP, WORKER_GROUP)
            //TODO support EpollSocketChannel for max performance on Linux
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
