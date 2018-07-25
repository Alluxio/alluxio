/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util.network;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.network.ChannelType;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.FileUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for working with Netty.
 */
@ThreadSafe
public final class NettyUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NettyUtils.class);

  public static final ChannelType USER_CHANNEL_TYPE =
      getChannelType(PropertyKey.USER_NETWORK_NETTY_CHANNEL);
  public static final ChannelType WORKER_CHANNEL_TYPE =
      getChannelType(PropertyKey.WORKER_NETWORK_NETTY_CHANNEL);

  private NettyUtils() {}

  /**
   * Creates a Netty {@link EventLoopGroup} based on {@link ChannelType}.
   *
   * @param type Selector for which form of low-level IO we should use
   * @param numThreads target number of threads
   * @param threadPrefix name pattern for each thread. should contain '%d' to distinguish between
   *        threads.
   * @param isDaemon if true, the {@link java.util.concurrent.ThreadFactory} will create daemon
   *        threads.
   * @return EventLoopGroup matching the ChannelType
   */
  public static EventLoopGroup createEventLoop(ChannelType type, int numThreads,
      String threadPrefix, boolean isDaemon) {
    ThreadFactory threadFactory = ThreadFactoryUtils.build(threadPrefix, isDaemon);

    switch (type) {
      case NIO:
        return new NioEventLoopGroup(numThreads, threadFactory);
      case EPOLL:
        return new EpollEventLoopGroup(numThreads, threadFactory);
      default:
        throw new IllegalArgumentException("Unknown io type: " + type);
    }
  }

  /**
   * Returns the correct {@link io.netty.channel.socket.ServerSocketChannel} class based on
   * {@link ChannelType}.
   *
   * @param type Selector for which form of low-level IO we should use
   * @param isDomainSocket whether this is a domain socket server
   * @return ServerSocketChannel matching the requirements
   */
  public static Class<? extends ServerChannel> getChannelClass(ChannelType type,
      boolean isDomainSocket) {
    if (isDomainSocket) {
      Preconditions.checkState(type == ChannelType.EPOLL,
          "Domain sockets are only supported with EPOLL channel type.");
      return EpollServerDomainSocketChannel.class;
    }
    switch (type) {
      case NIO:
        return NioServerSocketChannel.class;
      case EPOLL:
        return EpollServerSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io type: " + type);
    }
  }

  /**
   * Enables auto read for a netty channel.
   *
   * @param channel the netty channel
   */
  public static void enableAutoRead(Channel channel) {
    if (!channel.config().isAutoRead()) {
      channel.config().setAutoRead(true);
      channel.read();
    }
  }

  /**
   * Disables auto read for a netty channel.
   *
   * @param channel the netty channel
   */
  public static void disableAutoRead(Channel channel) {
    channel.config().setAutoRead(false);
  }

  /**
   * @param workerNetAddress the worker address
   * @return true if the domain socket is enabled on this client
   */
  public static boolean isDomainSocketSupported(WorkerNetAddress workerNetAddress) {
    if (workerNetAddress.getDomainSocketPath().isEmpty()
        || USER_CHANNEL_TYPE != ChannelType.EPOLL) {
      return false;
    }
    if (Configuration.getBoolean(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID)) {
      return FileUtils.exists(workerNetAddress.getDomainSocketPath());
    } else {
      return workerNetAddress.getHost().equals(NetworkAddressUtils.getClientHostName());
    }
  }

  /**
   * Note: Packet streaming requires {@link io.netty.channel.epoll.EpollMode} to be set to
   * LEVEL_TRIGGERED which is not supported in netty versions < 4.0.26.Final. Without shading netty
   * in Alluxio, we cannot use epoll.
   *
   * @param key the property key for looking up the configured channel type
   * @return the channel type to use
   */
  private static ChannelType getChannelType(PropertyKey key) {
    ChannelType configured = Configuration.getEnum(key, ChannelType.class);
    if (configured == ChannelType.EPOLL) {
      if (!Epoll.isAvailable()) {
        LOG.info("EPOLL is not available, will use NIO");
        return ChannelType.NIO;
      }
      try {
        EpollChannelOption.class.getField("EPOLL_MODE");
      } catch (Throwable e) {
        LOG.warn("EPOLL_MODE is not supported in netty with version < 4.0.26.Final, will use NIO");
        return ChannelType.NIO;
      }
    }
    return configured;
  }
}
