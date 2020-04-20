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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
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
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
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

  private static Boolean sNettyEpollAvailable = null;

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
   * Returns the correct {@link io.netty.channel.socket.ServerSocketChannel} class for use by the
   * worker.
   *
   * @param isDomainSocket whether this is a domain socket server
   * @param conf Alluxio configuration
   * @return ServerSocketChannel matching the requirements
   */
  public static Class<? extends ServerChannel> getServerChannelClass(boolean isDomainSocket,
      AlluxioConfiguration conf) {
    ChannelType workerChannelType = getWorkerChannel(conf);
    if (isDomainSocket) {
      Preconditions.checkState(workerChannelType == ChannelType.EPOLL,
          "Domain sockets are only supported with EPOLL channel type.");
      return EpollServerDomainSocketChannel.class;
    }
    switch (workerChannelType) {
      case NIO:
        return NioServerSocketChannel.class;
      case EPOLL:
        return EpollServerSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io type: " + workerChannelType);
    }
  }

  /**
   * @param workerNetAddress the worker address
   * @param conf Alluxio configuration
   * @return true if the domain socket is enabled on this client
   */
  public static boolean isDomainSocketAccessible(WorkerNetAddress workerNetAddress,
      AlluxioConfiguration conf) {
    if (!isDomainSocketSupported(workerNetAddress) || getUserChannel(conf) != ChannelType.EPOLL) {
      return false;
    }
    if (conf.getBoolean(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID)) {
      return FileUtils.exists(workerNetAddress.getDomainSocketPath());
    } else {
      return workerNetAddress.getHost().equals(NetworkAddressUtils.getClientHostName(conf));
    }
  }

  /**
   * @param workerNetAddress the worker address
   * @return true if the domain socket is supported by the worker
   */
  public static boolean isDomainSocketSupported(WorkerNetAddress workerNetAddress) {
    return !workerNetAddress.getDomainSocketPath().isEmpty();
  }

  /**
   * @return whether netty epoll is available to the system
   */
  public static synchronized boolean isNettyEpollAvailable() {
    if (sNettyEpollAvailable == null) {
      // Only call checkNettyEpollAvailable once ever so that we only log the result once.
      sNettyEpollAvailable = checkNettyEpollAvailable();
    }
    return sNettyEpollAvailable;
  }

  private static boolean checkNettyEpollAvailable() {
    if (!Epoll.isAvailable()) {
      LOG.info("EPOLL is not available, will use NIO");
      return false;
    }
    try {
      EpollChannelOption.class.getField("EPOLL_MODE");
      LOG.info("EPOLL_MODE is available");
      return true;
    } catch (Throwable e) {
      LOG.warn("EPOLL_MODE is not supported in netty with version < 4.0.26.Final, will use NIO");
      return false;
    }
  }

  /**
   * Gets the ChannelType properly from the USER_NETWORK_NETTY_CHANNEL property key.
   *
   * @param conf Alluxio configuration
   * @return the proper channel type USER_NETWORK_NETTY_CHANNEL
   */
  public static ChannelType getUserChannel(AlluxioConfiguration conf) {
    return getChannelType(PropertyKey.USER_NETWORK_STREAMING_NETTY_CHANNEL, conf);
  }

  /**
   * Gets the worker channel properly from the WORKER_NETWORK_NETTY_CHANNEL property key.
   *
   * @param conf Alluxio configuration
   * @return the proper channel type for WORKER_NETWORK_NETY_CHANNEL
   */
  public static ChannelType getWorkerChannel(AlluxioConfiguration conf) {
    return getChannelType(PropertyKey.WORKER_NETWORK_NETTY_CHANNEL, conf);
  }

  /**
   * Get the proper channel class.
   * Always returns {@link NioSocketChannel} NIO if EPOLL is not available.
   *
   * @param isDomainSocket whether this is for a domain channel
   * @param key the property key for looking up the configured channel type
   * @param conf the Alluxio configuration
   * @return the channel type to use
   */
  public static Class<? extends Channel> getChannelClass(boolean isDomainSocket, PropertyKey key,
      AlluxioConfiguration conf) {
    ChannelType channelType = getChannelType(key, conf);
    if (isDomainSocket) {
      Preconditions.checkState(channelType == ChannelType.EPOLL,
          "Domain sockets are only supported with EPOLL channel type.");
      return EpollDomainSocketChannel.class;
    }
    switch (channelType) {
      case NIO:
        return NioSocketChannel.class;
      case EPOLL:
        return EpollSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io type: " + channelType);
    }
  }

  /**
   * Get the proper channel type. Always returns {@link ChannelType} NIO if EPOLL is not available.
   *
   * @param key the property key for looking up the configured channel type
   * @param conf the Alluxio configuration
   * @return the channel type to use
   */
  public static ChannelType getChannelType(PropertyKey key, AlluxioConfiguration conf) {
    if (!isNettyEpollAvailable()) {
      return ChannelType.NIO;
    }
    return conf.getEnum(key, ChannelType.class);
  }
}
