package alluxio.grpc;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.collections.Pair;
import alluxio.resource.LockResource;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Verify;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  private static final Random RANDOM = new Random();

  // Singleton instance.
  private static GrpcManagedChannelPool sInstance;

  static {
    sInstance = new GrpcManagedChannelPool();
  }

  /**
   * @return the singleton pool instance.
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
  private HashMap<ChannelKey, ManagedChannelReference> mChannels;
  /** Used to control access to mChannel */
  private ReentrantReadWriteLock mLock;

  /** Scheduler for destruction of idle channels. */
  protected ScheduledExecutorService mScheduler;

  /**
   * Creates a new {@link GrpcManagedChannelPool}.
   */
  public GrpcManagedChannelPool() {
    mChannels = new HashMap<>();
    mLock = new ReentrantReadWriteLock(true);

    startScheduler();
  }

  /**
   * Restarts the pool by restarting the termination scheduler.
   */
  public void restart() throws InterruptedException {

    try (LockResource lockExclusive = new LockResource(mLock.writeLock())) {
      mScheduler.shutdown();
      // Wait for each channel termination upto configured single channel timeout
      long singleChannelTimeoutMs =
          Configuration.getMs(PropertyKey.MASTER_GRPC_CHANNEL_SHUTDOWN_TIMEOUT);
      long waitTimeMs = mChannels.size() * singleChannelTimeoutMs;
      mScheduler.awaitTermination(waitTimeMs, TimeUnit.MILLISECONDS);
      mScheduler.shutdownNow();
    }

    startScheduler();
  }

  private void startScheduler() {

    mScheduler = Executors.newScheduledThreadPool(1,
        ThreadFactoryUtils.build("grpc-channel-terminator", true));
    // Channel termination callback will be fired in the same interval as
    // the channel shutdown timeout.
    long channelTerminationIntervalMs =
        Configuration.getMs(PropertyKey.MASTER_GRPC_CHANNEL_SHUTDOWN_TIMEOUT);
    mScheduler.scheduleAtFixedRate(() -> {
      destroyInactiveChannels();
    }, channelTerminationIntervalMs, channelTerminationIntervalMs, TimeUnit.MILLISECONDS);
  }

  private void destroyInactiveChannels() {
    int channelCount = 0;
    int destroyedCount = 0;
    List<Pair<ChannelKey, ManagedChannelReference>> channelsToDestroy = new ArrayList<>();
    try (LockResource lockExclusive = new LockResource(mLock.writeLock())) {
      channelCount = mChannels.size();
      for (HashMap.Entry<ChannelKey, ManagedChannelReference> channelEntry : mChannels.entrySet()) {
        ChannelKey channelKey = channelEntry.getKey();
        ManagedChannelReference channelReference = channelEntry.getValue();
        if (channelReference.getRefCount() <= 0) {
          mChannels.remove(channelKey);
          channelsToDestroy.add(new Pair(channelKey, channelReference));
        }
      }
    }

    // TODO(ggezer) Consider shutting down in parallel.
    for (Pair<ChannelKey, ManagedChannelReference> channelPair : channelsToDestroy) {
      ManagedChannel channel = channelPair.getSecond().get();
      channel.shutdown();
      try {
        channel.awaitTermination(
            Configuration.getMs(PropertyKey.MASTER_GRPC_CHANNEL_SHUTDOWN_TIMEOUT),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Allow thread to exit.
      } finally {
        channel.shutdownNow();
      }
      Verify.verify(channel.isShutdown());
      destroyedCount++;
      LOG.debug("Destroyed gRPC managed channel for key:{}", channelPair.getFirst());
    }
    LOG.debug("gRPC channel terminator shut down {} of {} total channels.", destroyedCount,
        channelCount);
  }

  /**
   * Acquires and Increases the reference count for the singleton {@link ManagedChannel}.
   *
   * @param channelKey channel key
   * @return a {@link ManagedChannel}
   */
  public ManagedChannel acquireManagedChannel(ChannelKey channelKey) {
    try (LockResource lockShared = new LockResource(mLock.readLock())) {
      if (mChannels.containsKey(channelKey)) {
        return mChannels.get(channelKey).reference();
      }
    }
    try (LockResource lockExclusive = new LockResource(mLock.writeLock())) {
      if (!mChannels.containsKey(channelKey)) {
        mChannels.put(channelKey, new ManagedChannelReference(createManagedChannel(channelKey)));
      }
      return mChannels.get(channelKey).reference();
    }
  }

  /**
   * Decreases the reference count for the singleton {@link ManagedChannel} for the given address.
   *
   * It releases the {@link ManagedChannel} if reference count reaches zero.
   *
   * @param channelKey host address
   */
  public void releaseManagedChannel(ChannelKey channelKey) {
    try (LockResource lockShared = new LockResource(mLock.readLock())) {
      Verify.verify(mChannels.containsKey(channelKey));
      mChannels.get(channelKey).dereference();
    }
  }

  /**
   * Creates a {@link ManagedChannel} by given pool key.
   *
   * @param channelKey channel pool key
   * @return the created channel
   */
  private ManagedChannel createManagedChannel(ChannelKey channelKey) {
    NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(channelKey.mAddress);
    if (channelKey.mKeepAliveTime.isPresent()) {
      channelBuilder.keepAliveTime(channelKey.mKeepAliveTime.get().getFirst(),
          channelKey.mKeepAliveTime.get().getSecond());
    }
    if (channelKey.mKeepAliveTimeout.isPresent()) {
      channelBuilder.keepAliveTimeout(channelKey.mKeepAliveTimeout.get().getFirst(),
          channelKey.mKeepAliveTimeout.get().getSecond());
    }
    if (channelKey.mMaxInboundMessageSize.isPresent()) {
      channelBuilder.maxInboundMetadataSize(channelKey.mMaxInboundMessageSize.get());
    }
    if (channelKey.mFlowControlWindow.isPresent()) {
      channelBuilder.flowControlWindow(channelKey.mFlowControlWindow.get());
    }
    if (channelKey.mChannelType.isPresent()) {
      channelBuilder.channelType(channelKey.mChannelType.get());
    }
    if (channelKey.mEventLoopGroup.isPresent()) {
      channelBuilder.eventLoopGroup(channelKey.mEventLoopGroup.get());
    }
    if (channelKey.mPlain) {
      channelBuilder.usePlaintext();
    }
    return channelBuilder.build();
  }

  /**
   * Used as reference counting wrapper over {@link ManagedChannel}.
   */
  private class ManagedChannelReference {
    private ManagedChannel mChannel;
    private AtomicInteger mRefCount;

    private ManagedChannelReference(ManagedChannel channel) {
      mChannel = channel;
      mRefCount = new AtomicInteger(0);
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
     * @return current ref-count.
     */
    private int getRefCount() {
      return mRefCount.get();
    }

    /**
     * @return the underlying {@link ManagedChannel} without changing the ref-count
     */
    private ManagedChannel get(){
      return mChannel;
    }
  }

  public enum PoolingStrategy {
    DEFAULT,
    DISABLED
  }

  /**
   * Used to identify a unique {@link ManagedChannel} in the pool.
   */
  public static class ChannelKey {
    private SocketAddress mAddress;
    private boolean mPlain = true;
    private Optional<Pair<Long, TimeUnit>> mKeepAliveTime = Optional.empty();
    private Optional<Pair<Long, TimeUnit>> mKeepAliveTimeout = Optional.empty();
    private Optional<Integer> mMaxInboundMessageSize = Optional.empty();
    private Optional<Integer> mFlowControlWindow = Optional.empty();
    private Optional<Class<? extends io.netty.channel.Channel>> mChannelType = Optional.empty();
    private Optional<EventLoopGroup> mEventLoopGroup = Optional.empty();
    private long mPoolKey = 0;
    private ChannelKey() {}

    public static ChannelKey create() {
      return new ChannelKey();
    }

    /**
     * @param address destination address of the channel
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey setAddress(SocketAddress address) {
      mAddress = address;
      return this;
    }

    /**
     * Plaintext channel with no transport security.
     * 
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey usePlaintext() {
      mPlain = true;
      return this;
    }

    /**
     * @param keepAliveTime keep alive time for the underlying channel
     * @param timeUnit time unit for the keepAliveTime parameter
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey setKeepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
      mKeepAliveTime = Optional.of(new Pair<>(keepAliveTime, timeUnit));
      return this;
    }

    /**
     * @param keepAliveTimeout keep alive timeout for the underlying channel
     * @param timeUnit time unit for the keepAliveTimeout parameter
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey setKeepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
      mKeepAliveTimeout = Optional.of(new Pair<>(keepAliveTimeout, timeUnit));
      return this;
    }

    /**
     * @param maxInboundMessageSize Max inbound message size for the underlying channel
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey setMaxInboundMessageSize(int maxInboundMessageSize) {
      mMaxInboundMessageSize = Optional.of(maxInboundMessageSize);
      return this;
    }

    /**
     * @param flowControlWindow flow control window value for the underlying channel
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey setFlowControlWindow(int flowControlWindow) {
      mFlowControlWindow = Optional.of(flowControlWindow);
      return this;
    }

    /**
     *
     * @param channelType channel type for the underlying channel
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey setChannelType(Class<? extends io.netty.channel.Channel> channelType) {
      mChannelType = Optional.of(channelType);
      return this;
    }

    /**
     *
     * @param eventLoopGroup event loop group for the underlying channel
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey setEventLoopGroup(EventLoopGroup eventLoopGroup) {
      mEventLoopGroup = Optional.of(eventLoopGroup);
      return this;
    }

    /**
     *
     * @param strategy the pooling strategy
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey setPoolingStrategy(PoolingStrategy strategy) {
      // TODO(feng): implement modularized pooling strategies
      switch (strategy) {
        case DEFAULT:
          mPoolKey = 0;
          break;
        case DISABLED:
          mPoolKey = RANDOM.nextLong();
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Invalid pooling strategy %s", strategy.name()));
      }
      return this;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder()
          .append(mAddress)
          .append(mPlain)
          .append(mKeepAliveTime)
          .append(mKeepAliveTimeout)
          .append(mMaxInboundMessageSize)
          .append(mFlowControlWindow)
          .append(mPoolKey)
          .append(
              mChannelType.isPresent() ? System.identityHashCode(mChannelType.get()) : null)
          .append(
              mEventLoopGroup.isPresent() ? System.identityHashCode(mEventLoopGroup.get()) : null)
          .toHashCode();
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof ChannelKey) {
        ChannelKey otherKey = (ChannelKey) other;
        return mAddress.equals(otherKey.mAddress)
            && mPlain == otherKey.mPlain
            && mKeepAliveTime.equals(otherKey.mKeepAliveTime)
            && mKeepAliveTimeout.equals(otherKey.mKeepAliveTimeout)
            && mFlowControlWindow.equals(otherKey.mFlowControlWindow)
            && mMaxInboundMessageSize.equals(otherKey.mMaxInboundMessageSize)
            && mChannelType.equals(otherKey.mChannelType)
            && mPoolKey == otherKey.mPoolKey
            && mEventLoopGroup.equals(otherKey.mEventLoopGroup);
      }
      return false;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("Address", mAddress)
          .add("IsPlain", mPlain)
          .add("KeepAliveTime", mKeepAliveTime)
          .add("KeepAliveTimeout", mKeepAliveTimeout)
          .add("FlowControlWindow", mFlowControlWindow)
          .add("ChannelType", mChannelType)
          .add("EventLoopGroup", mEventLoopGroup)
          .toString();
    }
  }
}
