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

package alluxio.grpc;

import alluxio.collections.Pair;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Preconditions;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used to maintain singleton gRPC {@link ManagedChannel} instances per process.
 *
 * This class is used internally by {@link GrpcChannelBuilder} and {@link GrpcChannel}.
 */
@ThreadSafe
public class GrpcManagedChannelPool {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcManagedChannelPool.class);

  // Singleton instance.
  private static GrpcManagedChannelPool sInstance;

  static {
    sInstance = new GrpcManagedChannelPool();
  }

  /** Channels per address. */
  private ConcurrentMap<GrpcChannelKey, ManagedChannelHolder> mChannels;

  /**
   * Creates a new {@link GrpcManagedChannelPool}.
   */
  public GrpcManagedChannelPool() {
    mChannels = new ConcurrentHashMap<>();
  }

  /**
   * @return the singleton pool instance
   */
  public static GrpcManagedChannelPool INSTANCE() {
    return sInstance;
  }

  /**
   * Acquires and increases the ref-count for the {@link ManagedChannel}.
   *
   * @param channelKey channel key
   * @param healthCheckTimeoutMs health check timeout in milliseconds
   * @param shutdownTimeoutMs shutdown timeout in milliseconds
   * @return a {@link ManagedChannel}
   */
  public ManagedChannel acquireManagedChannel(GrpcChannelKey channelKey, long healthCheckTimeoutMs,
      long shutdownTimeoutMs) {
    return mChannels.compute(channelKey, (key, chHolder) -> {
      boolean shutdownExistingChannel = false;
      int existingRefCount = 0;
      if (chHolder != null) {
        if (waitForChannelReady(chHolder.get(), healthCheckTimeoutMs)) {
          LOG.debug("Acquiring an existing managed channel. ChannelKey: {}. Ref-count: {}", key,
              chHolder.getRefCount());
          return chHolder.reference();
        } else {
          shutdownExistingChannel = true;
        }
      }
      if (shutdownExistingChannel) {
        // TODO(ggezer): Avoid being have to create new channel with existing ref-count.
        existingRefCount = chHolder.getRefCount();
        LOG.debug("Shutting down an existing unhealthy managed channel. "
            + "ChannelKey: {}. Existing Ref-count: {}", key, existingRefCount);
        // Shutdown the channel forcefully as it's already unhealthy.
        forceShutdownManagedChannel(chHolder.get(), shutdownTimeoutMs);
      }

      LOG.debug("Creating a new managed channel. ChannelKey: {}. Ref-count:{}", key,
          existingRefCount);
      return new ManagedChannelHolder(createManagedChannel(key), existingRefCount).reference();
    }).get();
  }

  /**
   * Decreases the ref-count of the {@link ManagedChannel} for the given address.
   * It shuts down the underlying channel if reference count reaches zero.
   *
   * @param channelKey host address
   * @param shutdownTimeoutMs shutdown timeout in milliseconds
   */
  public void releaseManagedChannel(GrpcChannelKey channelKey, long shutdownTimeoutMs) {
    mChannels.compute(channelKey, (key, chHolder) -> {
      Preconditions.checkNotNull(chHolder, "Releasing nonexistent channel");
      if (chHolder.dereference() == 0) {
        LOG.debug("Released managed channel for: {}. Ref-count: {}", key, chHolder.getRefCount());
        // Shutdown the channel gracefully.
        shutdownManagedChannel(chHolder.get(), shutdownTimeoutMs);
        return null;
      }
      return chHolder;
    });
  }

  /**
   * Creates a {@link ManagedChannel} by given pool key.
   */
  private ManagedChannel createManagedChannel(GrpcChannelKey channelKey) {
    // Create netty channel builder with the address from channel key.
    NettyChannelBuilder channelBuilder;
    SocketAddress address = channelKey.getServerAddress().getSocketAddress();
    if (address instanceof InetSocketAddress) {
      InetSocketAddress inetServerAddress = (InetSocketAddress) address;
      // This constructor delays DNS lookup to detect changes
      channelBuilder = NettyChannelBuilder.forAddress(inetServerAddress.getHostName(),
          inetServerAddress.getPort());
    } else {
      channelBuilder = NettyChannelBuilder.forAddress(address);
    }

    // Initialize netty channel builder with the values from channel key.
    Optional<Pair<Long, TimeUnit>> keepAliveTime = channelKey.getKeepAliveTime();
    if (keepAliveTime.isPresent()) {
      channelBuilder.keepAliveTime(keepAliveTime.get().getFirst(), keepAliveTime.get().getSecond());
    }
    Optional<Pair<Long, TimeUnit>> keepAliveTimeout = channelKey.getKeepAliveTimeout();
    if (keepAliveTimeout.isPresent()) {
      channelBuilder.keepAliveTimeout(keepAliveTimeout.get().getFirst(),
          keepAliveTimeout.get().getSecond());
    }
    Optional<Integer> maxInboundMsgSize = channelKey.getMaxInboundMessageSize();
    if (maxInboundMsgSize.isPresent()) {
      channelBuilder.maxInboundMessageSize(maxInboundMsgSize.get());
    }
    Optional<Integer> flowControlWindow = channelKey.getFlowControlWindow();
    if (flowControlWindow.isPresent()) {
      channelBuilder.flowControlWindow(flowControlWindow.get());
    }
    Optional<Class<? extends Channel>> channelType = channelKey.getChannelType();
    if (channelType.isPresent()) {
      channelBuilder.channelType(channelType.get());
    }
    Optional<EventLoopGroup> eventLoopGroup = channelKey.getEventLoopGroup();
    if (eventLoopGroup.isPresent()) {
      channelBuilder.eventLoopGroup(eventLoopGroup.get());
    }
    // Use plaintext
    channelBuilder.usePlaintext();
    // Build netty managed channel.
    return channelBuilder.build();
  }

  /**
   * Returns {@code true} if given managed channel is ready.
   */
  private boolean waitForChannelReady(ManagedChannel managedChannel, long healthCheckTimeoutMs) {
    try {
      Boolean res = CommonUtils.waitForResult("channel to be ready", () -> {
        ConnectivityState currentState = managedChannel.getState(true);
        switch (currentState) {
          case READY:
            return true;
          case TRANSIENT_FAILURE:
          case SHUTDOWN:
            return false;
          case IDLE:
          case CONNECTING:
            return null;
          default:
            return null;
        }
      }, WaitForOptions.defaults().setTimeoutMs((int) healthCheckTimeoutMs));
      return res;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  /**
   * Tries to gracefully shut down the managed channel.
   * If falls back to forceful shutdown if graceful shutdown times out.
   */
  private void shutdownManagedChannel(ManagedChannel managedChannel, long shutdownTimeoutMs) {
    managedChannel.shutdown();
    try {
      if (!managedChannel.awaitTermination(shutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
        LOG.warn("Timed out gracefully shutting down managed channel: {}. ", managedChannel);
        // Forcefully shut down.
        forceShutdownManagedChannel(managedChannel, shutdownTimeoutMs);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // Allow thread to exit.
    }
  }

  /**
   * Forcefully shuts down the managed channel.
   */
  private void forceShutdownManagedChannel(ManagedChannel managedChannel, long shutdownTimeoutMs) {
    managedChannel.shutdownNow();
    try {
      if (!managedChannel.awaitTermination(shutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
        LOG.warn("Timed out forcefully shutting down managed channel: {}. ", managedChannel);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // Allow thread to exit.
    }
  }

  /**
   * Used as reference counting wrapper over {@link ManagedChannel}.
   */
  private class ManagedChannelHolder {
    private ManagedChannel mChannel;
    private AtomicInteger mRefCount;

    private ManagedChannelHolder(ManagedChannel channel, int refCount) {
      mChannel = channel;
      mRefCount = new AtomicInteger(refCount);
    }

    /**
     * @return the underlying {@link ManagedChannel} after increasing ref-count
     */
    private ManagedChannelHolder reference() {
      mRefCount.incrementAndGet();
      return this;
    }

    /**
     * Decrement the ref-count for underlying {@link ManagedChannel}.
     *
     * @return the current ref count after dereference
     */
    private int dereference() {
      return mRefCount.decrementAndGet();
    }

    /**
     * @return current ref-count
     */
    private int getRefCount() {
      return mRefCount.get();
    }

    /**
     * @return the underlying {@link ManagedChannel} without changing the ref-count
     */
    private ManagedChannel get() {
      return mChannel;
    }
  }
}
