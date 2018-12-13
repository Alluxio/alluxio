package alluxio.grpc;

import alluxio.resource.LockResource;
import com.google.common.base.Verify;
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
  private static HashMap<InetSocketAddress, ManagedChannelReference> mChannels;
  /** Used to control access to mChannel */
  private static ReentrantReadWriteLock mLock;

  static {
    mChannels = new HashMap<>();
    mLock = new ReentrantReadWriteLock(true);
  }

  /**
   * Acquires and Increases the reference count for the singleton {@link ManagedChannel}.
   * 
   * @param address host address
   * @return a {@link ManagedChannel}
   */
  public static ManagedChannel acquireManagedChannel(InetSocketAddress address) {
    try (LockResource lockShared = new LockResource(mLock.readLock())) {
      if (mChannels.containsKey(address)) {
        return mChannels.get(address).reference();
      }
    }
    try (LockResource lockExclusive = new LockResource(mLock.writeLock())) {
      if (!mChannels.containsKey(address)) {
        mChannels.put(address, new ManagedChannelReference(
            NettyChannelBuilder.forAddress(address).usePlaintext().build()));
      }
      return mChannels.get(address).reference();
    }
  }

  /**
   * Decreases the reference count for the singleton {@link ManagedChannel} for the given address.
   *
   * It releases the {@link ManagedChannel} if reference count reaches zero.
   *
   * @param address host address
   */
  public static void releaseManagedChannel(InetSocketAddress address) {

    boolean disposeChannel = false;
    try (LockResource lockShared = new LockResource(mLock.readLock())) {
      if (mChannels.containsKey(address) && mChannels.get(address).dereference()) {
        disposeChannel = true;
      }
    }

    if (disposeChannel) {
      try (LockResource lockExclusive = new LockResource(mLock.writeLock())) {
        if (mChannels.containsKey(address)) {
          mChannels.get(address).reference();
          if (mChannels.get(address).dereference()) {
            ManagedChannel channel = mChannels.remove(address).reference();
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
}
