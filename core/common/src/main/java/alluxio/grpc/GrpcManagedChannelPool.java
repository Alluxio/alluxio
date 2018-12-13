package alluxio.grpc;

import alluxio.resource.LockResource;
import com.google.common.base.Verify;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

import javax.annotation.concurrent.GuardedBy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Used to maintain singleton gRPC {@link ManagedChannel} instances per process.
 *
 * This class is used internally by {@link GrpcChannelBuilder} and {@link GrpcChannel}.
 */
public class GrpcManagedChannelPool {
  /** Channels per address. */
  @GuardedBy("mLock")
  private static HashMap<ChannelKey, ManagedChannelReference> mChannels;
  /** Used to control access to mChannel */
  private static ReentrantReadWriteLock mLock;

  static {
    mChannels = new HashMap<>();
    mLock = new ReentrantReadWriteLock(true);
  }

  /**
   * Acquires and Increases the reference count for the singleton {@link ManagedChannel}.
   *
   * @param channelKey channel key
   * @return a {@link ManagedChannel}
   */
  public static ManagedChannel acquireManagedChannel(ChannelKey channelKey) {
    try (LockResource lockShared = new LockResource(mLock.readLock())) {
      if (mChannels.containsKey(channelKey)) {
        return mChannels.get(channelKey).reference();
      }
    }
    try (LockResource lockExclusive = new LockResource(mLock.writeLock())) {
      if (!mChannels.containsKey(channelKey)) {
        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(channelKey.mAddress);
        if (channelKey.mPlain) {
          channelBuilder.usePlaintext();
        }

        mChannels.put(channelKey, new ManagedChannelReference(channelBuilder.build()));
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
  public static void releaseManagedChannel(ChannelKey channelKey) {

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
   * Used as reference counting wrapper over {@link ManagedChannel}.
   */
  private static class ManagedChannelReference {
    private ManagedChannel mChannel;
    private AtomicInteger mRefCount;

    private ManagedChannelReference(ManagedChannel channel) {
      mChannel = channel;
      mRefCount = new AtomicInteger(0);
    }

    private ManagedChannel reference() {
      mRefCount.incrementAndGet();
      return mChannel;
    }

    private boolean dereference() {
      return mRefCount.decrementAndGet() <= 0;
    }
  }

  /**
   * Used to identify a unique {@link ManagedChannel} in the pool.
   */
  public static class ChannelKey {
    private InetSocketAddress mAddress;;
    private boolean mPlain;

    private ChannelKey() {}

    public static ChannelKey create() {
      return new ChannelKey();
    }

    /**
     * @param address destination address of the channel
     * @return the modified {@link ChannelKey}
     */
    public ChannelKey setAddress(InetSocketAddress address) {
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

    @Override
    public boolean equals(Object other) {
      if (other instanceof ChannelKey) {
        ChannelKey otherKey = (ChannelKey) other;
        return mAddress == otherKey.mAddress && mPlain == otherKey.mPlain;
      }
      return false;
    }
  }
}
