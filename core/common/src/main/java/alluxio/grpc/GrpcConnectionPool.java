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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.ChannelType;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NettyUtils;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Used to provide gRPC level connection management and pooling facilities.
 *
 * This class is used internally by {@link GrpcChannelBuilder} and {@link GrpcChannel}.
 */
@ThreadSafe
public class GrpcConnectionPool {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcConnectionPool.class);

  // Singleton instance.
  public static final GrpcConnectionPool INSTANCE = new GrpcConnectionPool();

  /** gRPC Managed channels/connections. */
  private Map<GrpcResourceKey.ManagedChannelKey, CountingReference<ManagedChannel>> mChannels;

  /** Event loops. */
  private Map<GrpcResourceKey.EventLoopGroupKey, CountingReference<EventLoopGroup>> mEventLoops;

  /** Used to assign order within a network group. */
  private Map<GrpcChannelKey.NetworkGroup, AtomicLong> mNetworkGroupCounters;

  /**
   * Creates a new {@link GrpcConnectionPool}.
   */
  public GrpcConnectionPool() {
    mChannels = new ConcurrentHashMap<>();
    mEventLoops = new ConcurrentHashMap<>();
    // Initialize counters for known network-groups.
    mNetworkGroupCounters = new ConcurrentHashMap<>();
    for (GrpcChannelKey.NetworkGroup group : GrpcChannelKey.NetworkGroup.values()) {
      mNetworkGroupCounters.put(group, new AtomicLong());
    }
  }

  /**
   * Acquires and increases the ref-count for the {@link ManagedChannel}.
   *
   * @param channelKey the channel key
   * @param conf the Alluxio configuration
   * @return a {@link GrpcConnection}
   */
  public GrpcConnection acquireConnection(GrpcChannelKey channelKey, AlluxioConfiguration conf) {
    // Get a connection key.
    GrpcResourceKey resourceKey = generateResourceKey(channelKey, conf);
    // Acquire connection.
    CountingReference<ManagedChannel> connectionRef =
        mChannels.compute(resourceKey.getManagedChannelKey(), (key, ref) -> {
          boolean shutdownExistingConnection = false;
          int existingRefCount = 0;
          if (ref != null) {
            // Connection exists, wait for health check.
            if (waitForConnectionReady(ref.get(), conf)) {
              LOG.debug("Acquiring an existing connection. ManagedChannelKey: {}. Ref-count: {}",
                  key, ref.getRefCount());
              return ref.reference();
            } else {
              // Health check failed.
              shutdownExistingConnection = true;
            }
          }
          // Existing connection should shut-down.
          if (shutdownExistingConnection) {
            // TODO(ggezer): Implement GrpcConnectionListener for receiving notification.
            existingRefCount = ref.getRefCount();
            LOG.debug("Shutting down an existing unhealthy connection. "
                + "ManagedChannelKey: {}. Ref-count: {}", key, existingRefCount);
            // Shutdown the channel forcefully as it's already unhealthy.
            shutdownManagedChannel(ref.get(), conf);
          }

          // Create a new managed channel.
          LOG.debug("Creating a new managed channel. ManagedChannelKey: {}. Ref-count:{}", key,
              existingRefCount);
          ManagedChannel managedChannel = createManagedChannel(resourceKey, conf);
          // Set map reference.
          return new CountingReference(managedChannel, existingRefCount).reference();
        });

    // Wrap connection reference and the connection.
    return new GrpcConnection(resourceKey, connectionRef.get(), conf);
  }

  /**
   * Decreases the ref-count of the {@link ManagedChannel} for the given address. It shuts down the
   * underlying channel if reference count reaches zero.
   *
   * @param resourceKey the connection key
   * @param conf the Alluxio configuration
   */
  public void releaseConnection(GrpcResourceKey resourceKey, AlluxioConfiguration conf) {
    mChannels.compute(resourceKey.getManagedChannelKey(), (key, ref) -> {
      Preconditions.checkNotNull(ref, "Cannot release nonexistent connection");
      LOG.debug("Releasing connection. ResourceKey: {}. Ref-count: {}",
          resourceKey, ref.getRefCount());
      // Shutdown managed channel.
      if (ref.dereference() == 0) {
        LOG.debug("Shutting down ManagedChannel: {}", key);
        shutdownManagedChannel(ref.get(), conf);
        // Release the event-loop for the connection.
        releaseNetworkEventLoop(resourceKey, conf);
        return null;
      }
      return ref;
    });
  }

  private GrpcResourceKey generateResourceKey(GrpcChannelKey channelKey,
      AlluxioConfiguration conf) {
    // Assign index within the network group.
    long groupIndex = mNetworkGroupCounters.get(channelKey.getNetworkGroup()).incrementAndGet();
    // Find the next slot index within the group.
    long maxConnectionsForGroup = conf.getLong(PropertyKey.Template.USER_NETWORK_MAX_CONNECTIONS
        .format(channelKey.getNetworkGroup().getPropertyCode()));
    int maxEventLoopGroupsForGroup =
        conf.getInt(PropertyKey.Template.USER_NETWORK_MAX_EVENTLOOP_GROUPS
            .format(channelKey.getNetworkGroup().getPropertyCode()));
    long connectionIndex = groupIndex % maxConnectionsForGroup;
    long eventLoopGroupIndex = connectionIndex % maxEventLoopGroupsForGroup;

    // Create the connection key for the chosen slot.
    return new GrpcResourceKey(channelKey, (int) connectionIndex, (int) eventLoopGroupIndex);
  }

  /**
   * Creates a {@link ManagedChannel} by given pool key.
   */
  private ManagedChannel createManagedChannel(GrpcResourceKey connectionKey,
      AlluxioConfiguration conf) {
    // Create netty channel builder with the address from channel key.
    NettyChannelBuilder channelBuilder;
    SocketAddress address =
        connectionKey.getClientChannelKey().getServerAddress().getSocketAddress();
    if (address instanceof InetSocketAddress) {
      InetSocketAddress inetServerAddress = (InetSocketAddress) address;
      // This constructor delays DNS lookup to detect changes
      channelBuilder = NettyChannelBuilder.forAddress(inetServerAddress.getHostName(),
          inetServerAddress.getPort());
    } else {
      channelBuilder = NettyChannelBuilder.forAddress(address);
    }
    // Apply default channel options for the multiplex group.
    channelBuilder = applyGroupDefaults(connectionKey, channelBuilder, conf);
    // Build netty managed channel.
    return channelBuilder.build();
  }

  /**
   * It updates and returns the given {@link NettyChannelBuilder} based on network group settings.
   */
  private NettyChannelBuilder applyGroupDefaults(GrpcResourceKey connectionKey,
      NettyChannelBuilder channelBuilder, AlluxioConfiguration conf) {
    long keepAliveTimeMs = conf.getMs(PropertyKey.Template.USER_NETWORK_KEEPALIVE_TIME_MS
        .format(connectionKey.getClientChannelKey().getNetworkGroup().getPropertyCode()));
    long keepAliveTimeoutMs = conf.getMs(PropertyKey.Template.USER_NETWORK_KEEPALIVE_TIMEOUT_MS
        .format(connectionKey.getClientChannelKey().getNetworkGroup().getPropertyCode()));
    long inboundMessageSizeBytes =
        conf.getBytes(PropertyKey.Template.USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE
            .format(connectionKey.getClientChannelKey().getNetworkGroup().getPropertyCode()));
    long flowControlWindow = conf.getBytes(PropertyKey.Template.USER_NETWORK_FLOWCONTROL_WINDOW
        .format(connectionKey.getClientChannelKey().getNetworkGroup().getPropertyCode()));
    Class<? extends Channel> channelType = NettyUtils.getChannelClass(
        !(connectionKey.getClientChannelKey().getServerAddress()
            .getSocketAddress() instanceof InetSocketAddress),
        PropertyKey.Template.USER_NETWORK_NETTY_CHANNEL
            .format(connectionKey.getClientChannelKey().getNetworkGroup().getPropertyCode()),
        conf);
    EventLoopGroup eventLoopGroup = acquireNetworkEventLoop(connectionKey, conf);

    // Update the builder.
    channelBuilder.keepAliveTime(keepAliveTimeMs, TimeUnit.MILLISECONDS);
    channelBuilder.keepAliveTimeout(keepAliveTimeoutMs, TimeUnit.MILLISECONDS);
    channelBuilder.maxInboundMessageSize((int) inboundMessageSizeBytes);
    channelBuilder.flowControlWindow((int) flowControlWindow);
    channelBuilder.channelType(channelType);
    channelBuilder.eventLoopGroup(eventLoopGroup);
    // Use plaintext
    channelBuilder.usePlaintext();

    return channelBuilder;
  }

  /**
   * Returns {@code true} if given managed channel is ready.
   */
  private boolean waitForConnectionReady(ManagedChannel managedChannel, AlluxioConfiguration conf) {
    long healthCheckTimeoutMs = conf.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);
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
      }, (b) -> b != null, WaitForOptions.defaults().setTimeoutMs((int) healthCheckTimeoutMs));
      return res;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  /**
   * Tries to gracefully shut down the managed channel. If falls back to forceful shutdown if
   * graceful shutdown times out.
   */
  private void shutdownManagedChannel(ManagedChannel managedChannel, AlluxioConfiguration conf) {
    // Close the gRPC managed-channel if not shut down already.
    if (!managedChannel.isShutdown()) {
      long gracefulTimeoutMs = conf.getMs(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_GRACEFUL_TIMEOUT);
      managedChannel.shutdown();
      try {
        if (!managedChannel.awaitTermination(gracefulTimeoutMs, TimeUnit.MILLISECONDS)) {
          LOG.warn("Timed out gracefully shutting down connection: {}. ", managedChannel);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Allow thread to exit.
      }
    }
    // Forceful shut down if still not terminated.
    if (!managedChannel.isTerminated()) {
      long timeoutMs = conf.getMs(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_TIMEOUT);

      managedChannel.shutdownNow();
      try {
        if (!managedChannel.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
          LOG.warn("Timed out forcefully shutting down connection: {}. ", managedChannel);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Allow thread to exit.
      }
    }
  }

  private EventLoopGroup acquireNetworkEventLoop(GrpcResourceKey connectionKey,
      AlluxioConfiguration conf) {
    return mEventLoops.compute(connectionKey.getEventLoopGroupKey(), (k, v) -> {
      // Increment and return if event-loop found.
      if (v != null) {
        LOG.debug("Acquiring an existing event-loop. ConnectionKey: {}, Ref-Count:{}",
            connectionKey, k, v.getRefCount());
        v.reference();
        return v;
      }

      // Create a new event-loop.
      ChannelType nettyChannelType =
          NettyUtils.getChannelType(PropertyKey.Template.USER_NETWORK_NETTY_CHANNEL.format(
              connectionKey.getClientChannelKey().getNetworkGroup().getPropertyCode()), conf);
      int nettyWorkerThreadCount =
          conf.getInt(PropertyKey.Template.USER_NETWORK_NETTY_WORKER_THREADS
              .format(connectionKey.getClientChannelKey().getNetworkGroup().getPropertyCode()));

      v = new CountingReference<>(NettyUtils.createEventLoop(nettyChannelType,
          nettyWorkerThreadCount, String.format("alluxio-client-netty-event-loop-%s-%%d",
              connectionKey.getClientChannelKey().getNetworkGroup().name()),
          true), 1);
      LOG.debug("Created a new event loop. ChannelKey: {}. ChannelType: {} ThreadCount: {}",
          connectionKey, nettyChannelType, nettyWorkerThreadCount);

      return v;
    }).get();
  }

  private void releaseNetworkEventLoop(GrpcResourceKey connectionKey, AlluxioConfiguration conf) {
    mEventLoops.compute(connectionKey.getEventLoopGroupKey(), (k, ref) -> {
      Preconditions.checkNotNull(ref, "Cannot release nonexistent event-loop");
      LOG.debug("Releasing event-loop. ConnectionKey: {}, Ref-count: {}",
          connectionKey, ref.getRefCount());
      if (ref.dereference() == 0) {
        LOG.debug("Shutting down event-loop. ConnectionKey: {}", connectionKey);
        // Shutdown the event-loop gracefully.
        ref.get().shutdownGracefully();
        // No need to wait for event-loop shutdown.
        return null;
      }
      return ref;
    });
  }

  /**
   * Used as reference counting wrapper over instance of type {@link T}.
   */
  private class CountingReference<T> {
    private T mObject;
    private AtomicInteger mRefCount;

    private CountingReference(T object, int initialRefCount) {
      mObject = object;
      mRefCount = new AtomicInteger(initialRefCount);
    }

    /**
     * @return the underlying object after increasing ref-count
     */
    private CountingReference reference() {
      mRefCount.incrementAndGet();
      return this;
    }

    /**
     * Decrement the ref-count for underlying object.
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
     * @return the underlying object without changing the ref-count
     */
    private T get() {
      return mObject;
    }
  }
}
