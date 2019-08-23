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
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Verify;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

  /**
   * @return the singleton pool instance
   */
  public static GrpcManagedChannelPool INSTANCE() {
    return sInstance;
  }

  /*
   * Concurrently shutting down gRPC channels may cause incomplete RPC messages
   * bouncing back and forth between client and server. This lock is used to serialize
   * channel shut downs in a single JVM boundary.
   */
  /** Channels per address. */
  @GuardedBy("mLock")
  private HashMap<GrpcChannelKey, ManagedChannelReference> mChannels;
  /** Used to control access to mChannel. */
  private ReentrantReadWriteLock mLock;

  /** Scheduler for destruction of idle channels. */
  protected ScheduledExecutorService mScheduler;

  /**
   * Creates a new {@link GrpcManagedChannelPool}.
   */
  public GrpcManagedChannelPool() {
    mChannels = new HashMap<>();
    mLock = new ReentrantReadWriteLock(true);
  }

  /**
   * Shuts down the managed channel for given key.
   *
   * (Should be called with {@code mLock} acquired.)
   *
   * @param channelKey channel key
   * @param shutdownTimeoutMs shutdown timeout in miliseconds
   */
  private void shutdownManagedChannel(GrpcChannelKey channelKey, long shutdownTimeoutMs) {
    ManagedChannel managedChannel = mChannels.get(channelKey).get();
    managedChannel.shutdown();
    try {
      managedChannel.awaitTermination(shutdownTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // Allow thread to exit.
    } finally {
      managedChannel.shutdownNow();
    }
    Verify.verify(managedChannel.isShutdown());
    LOG.debug("Shut down managed channel. ChannelKey: {}", channelKey);
  }

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
   * Acquires and increases the ref-count for the {@link ManagedChannel}.
   *
   * @param channelKey channel key
   * @param healthCheckTimeoutMs health check timeout in milliseconds
   * @param shutdownTimeoutMs shutdown timeout in milliseconds
   * @return a {@link ManagedChannel}
   */
  public ManagedChannel acquireManagedChannel(GrpcChannelKey channelKey,
      long healthCheckTimeoutMs, long shutdownTimeoutMs) {
    boolean shutdownExistingChannel = false;
    ManagedChannelReference managedChannelRef = null;
    try (LockResource lockShared = new LockResource(mLock.readLock())) {
      if (mChannels.containsKey(channelKey)) {
        managedChannelRef = mChannels.get(channelKey);
        if (waitForChannelReady(managedChannelRef.get(),
            healthCheckTimeoutMs)) {
          LOG.debug("Acquiring an existing managed channel. ChannelKey: {}. Ref-count: {}",
              channelKey, managedChannelRef.getRefCount());
          return managedChannelRef.reference();
        } else {
          // Postpone channel shutdown under exclusive lock below.
          shutdownExistingChannel = true;
        }
      }
    }
    try (LockResource lockExclusive = new LockResource(mLock.writeLock())) {
      // Dispose existing channel if required.
      int existingRefCount = 0;
      if (shutdownExistingChannel && mChannels.containsKey(channelKey)
          && mChannels.get(channelKey) == managedChannelRef) {
        existingRefCount = managedChannelRef.getRefCount();
        LOG.debug("Shutting down an existing unhealthy managed channel. "
            + "ChannelKey: {}. Existing Ref-count: {}", channelKey, existingRefCount);
        shutdownManagedChannel(channelKey, shutdownTimeoutMs);
        mChannels.remove(channelKey);
      }
      if (!mChannels.containsKey(channelKey)) {
        LOG.debug("Creating a new managed channel. ChannelKey: {}. Ref-count:{}",
            channelKey, existingRefCount);
        mChannels.put(channelKey,
            new ManagedChannelReference(createManagedChannel(channelKey), existingRefCount));
      }
      return mChannels.get(channelKey).reference();
    }
  }

  /**
   * Decreases the ref-count of the {@link ManagedChannel} for the given address.
   *
   * It shuts down and releases the {@link ManagedChannel} if reference count reaches zero.
   *
   * @param channelKey host address
   * @param shutdownTimeoutMs shutdown timeout in milliseconds
   */
  public void releaseManagedChannel(GrpcChannelKey channelKey, long shutdownTimeoutMs) {
    boolean shutdownManagedChannel;
    try (LockResource lockShared = new LockResource(mLock.readLock())) {
      Verify.verify(mChannels.containsKey(channelKey));
      ManagedChannelReference channelRef = mChannels.get(channelKey);
      channelRef.dereference();
      shutdownManagedChannel = channelRef.getRefCount() <= 0;
      LOG.debug("Released managed channel for: {}. Ref-count: {}", channelKey,
          channelRef.getRefCount());
    }
    if (shutdownManagedChannel) {
      try (LockResource lockExclusive = new LockResource(mLock.writeLock())) {
        if (mChannels.containsKey(channelKey)) {
          ManagedChannelReference channelRef = mChannels.get(channelKey);
          if (channelRef.getRefCount() <= 0) {
            shutdownManagedChannel(channelKey, shutdownTimeoutMs);
          }
        }
      }
    }
  }

  /**
   * Creates a {@link ManagedChannel} by given pool key.
   *
   * (Should be called with {@code mLock} acquired.)
   *
   * @param channelKey channel pool key
   * @return the created channel
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
   * Used as reference counting wrapper over {@link ManagedChannel}.
   */
  private class ManagedChannelReference {
    private ManagedChannel mChannel;
    private AtomicInteger mRefCount;

    private ManagedChannelReference(ManagedChannel channel, int refCount) {
      mChannel = channel;
      mRefCount = new AtomicInteger(refCount);
    }

    /**
     * @return the underlying {@link ManagedChannel} after increasing ref-count
     */
    private ManagedChannel reference() {
      mRefCount.incrementAndGet();
      return mChannel;
    }

    /**
     * Decrement the ref-count for underlying {@link ManagedChannel}.
     */
    private void dereference() {
      mRefCount.decrementAndGet();
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
