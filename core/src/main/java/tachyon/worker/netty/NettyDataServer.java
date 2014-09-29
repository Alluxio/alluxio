package tachyon.worker.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import tachyon.conf.WorkerConf;
import tachyon.worker.BlocksLocker;
import tachyon.worker.DataServer;

/**
 * Runs a netty server that will response to block requests.
 */
public final class NettyDataServer implements DataServer {
  private final ServerBootstrap mBootstrap;

  private final ChannelFuture mChannelFuture;

  public NettyDataServer(final SocketAddress address, final BlocksLocker locker) {
    mBootstrap = createBootstrap().childHandler(new PipelineHandler(locker));

    try {
      mChannelFuture = mBootstrap.bind(address).sync();
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    mChannelFuture.channel().close().awaitUninterruptibly();
    mBootstrap.group().shutdownGracefully();
    mBootstrap.childGroup().shutdownGracefully();
  }

  private ServerBootstrap createBootstrap() {
    final WorkerConf conf = WorkerConf.get();
    ServerBootstrap boot = new ServerBootstrap();
    boot = setupGroups(boot, conf.NETTY_CHANNEL_TYPE);

    // use pooled buffers
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    boot.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    // set write buffer
    // this is the default, but its recommended to set it in case of change in future netty.
    boot.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, conf.NETTY_HIGH_WATER_MARK);
    boot.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, conf.NETTY_LOW_WATER_MARK);

    // more buffer settings
    if (conf.NETTY_BACKLOG.isPresent()) {
      boot.option(ChannelOption.SO_BACKLOG, conf.NETTY_BACKLOG.get());
    }
    if (conf.NETTY_SEND_BUFFER.isPresent()) {
      boot.option(ChannelOption.SO_SNDBUF, conf.NETTY_SEND_BUFFER.get());
    }
    if (conf.NETTY_RECIEVE_BUFFER.isPresent()) {
      boot.option(ChannelOption.SO_RCVBUF, conf.NETTY_RECIEVE_BUFFER.get());
    }
    return boot;
  }

  private ThreadFactory createThreadFactory(final String nameFormat) {
    return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
  }

  /**
   * Gets the port listening on.
   */
  @Override
  public int getPort() {
    // according to the docs, a InetSocketAddress is returned and the user must down-cast
    return ((InetSocketAddress) mChannelFuture.channel().localAddress()).getPort();
  }

  @Override
  public boolean isClosed() {
    return mBootstrap.group().isShutdown();
  }

  /**
   * Creates a default {@link io.netty.bootstrap.ServerBootstrap} where the channel and groups are
   * preset. Current channel type supported are nio and epoll.
   */
  private ServerBootstrap setupGroups(final ServerBootstrap boot, final ChannelType type) {
    ThreadFactory workerFactory = createThreadFactory("data-server-%d");
    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    switch (type) {
      case EPOLL:
        bossGroup = new EpollEventLoopGroup(0, workerFactory);
        workerGroup = bossGroup;
        boot.channel(EpollServerSocketChannel.class);
        break;
      default:
        bossGroup = new NioEventLoopGroup(0, workerFactory);
        workerGroup = bossGroup;
        boot.channel(NioServerSocketChannel.class);
    }
    boot.group(bossGroup, workerGroup);
    return boot;
  }
}
