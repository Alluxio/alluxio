package alluxio.grpc;

import alluxio.collections.Pair;
import alluxio.resource.LockResource;
import com.google.common.base.Verify;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Optional;
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

  /** Channels per address. */
  @GuardedBy("mLock")
  private HashMap<ChannelKey, ManagedChannelReference> mChannels;
  /** Used to control access to mChannel */
  private ReentrantReadWriteLock mLock;

  /**
   * Creates a new {@link GrpcManagedChannelPool}.
   */
  public GrpcManagedChannelPool() {
    mChannels = new HashMap<>();
    mLock = new ReentrantReadWriteLock(true);
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

    boolean disposeChannel = false;
    try (LockResource lockShared = new LockResource(mLock.readLock())) {
      if (mChannels.containsKey(channelKey) && mChannels.get(channelKey).dereference()) {
        disposeChannel = true;
      }
    }

    if (disposeChannel) {
      try (LockResource lockExclusive = new LockResource(mLock.writeLock())) {
        if (mChannels.containsKey(channelKey)) {
          mChannels.get(channelKey).reference();
          if (mChannels.get(channelKey).dereference()) {
            ManagedChannel channel = mChannels.remove(channelKey).reference();
            channel.shutdown();
            boolean isTerminated = false;
            while (!isTerminated) {
              try {
                isTerminated = channel.awaitTermination(5, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                // Keep trying to close the channel.
              }
            }
            channel.shutdownNow();
            Verify.verify(channel.isTerminated());
          }
        }
      }
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
     *
     * @return {@code true} if underlying object is no longer referenced
     */
    private boolean dereference() {
      return mRefCount.decrementAndGet() <= 0;
    }
  }

  /**
   * Used to identify a unique {@link ManagedChannel} in the pool.
   */
  public static class ChannelKey {
    private SocketAddress mAddress;
    private boolean mPlain = false;
    private Optional<Pair<Long, TimeUnit>> mKeepAliveTimeout = Optional.empty();
    private Optional<Integer> mMaxInboundMessageSize = Optional.empty();
    private Optional<Integer> mFlowControlWindow = Optional.empty();
    private Optional<Class<? extends io.netty.channel.Channel>> mChannelType = Optional.empty();
    private Optional<EventLoopGroup> mEventLoopGroup = Optional.empty();

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
     * @param keepAliveTimeout keep alive timeout for the underlying channel
     * @param timeUnit timeout for the keepAliveTimeout parameter
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

    @Override
    public boolean equals(Object other) {
      if (other instanceof ChannelKey) {
        ChannelKey otherKey = (ChannelKey) other;
        return mAddress == otherKey.mAddress
            && mPlain == otherKey.mPlain
            && mKeepAliveTimeout == otherKey.mKeepAliveTimeout
            && mFlowControlWindow == otherKey.mFlowControlWindow
            && mMaxInboundMessageSize == otherKey.mMaxInboundMessageSize
            && mChannelType == otherKey.mChannelType
            && mEventLoopGroup == otherKey.mEventLoopGroup;
      }
      return false;
    }
  }
}
