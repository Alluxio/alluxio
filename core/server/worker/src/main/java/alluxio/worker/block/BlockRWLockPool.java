package alluxio.worker.block;

import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.ResourcePool;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class for managing block read/write locks. After obtaining a lock with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
public final class BlockRWLockPool extends ResourcePool<ClientRWLock> {
    private static final Logger LOG = LoggerFactory.getLogger(BlockRWLockPool.class);

    @VisibleForTesting
    final AtomicBoolean mHasReachedFullCapacity;
    private final AtomicInteger mRemainingPoolResources;

    /**
     * Creates a new client read/write lock pool.
     */
    public BlockRWLockPool(int maxCapacity) {
        super(maxCapacity);
        mHasReachedFullCapacity = new AtomicBoolean(false);
        mRemainingPoolResources = new AtomicInteger(maxCapacity);

        MetricsSystem.registerGaugeIfAbsent(
                MetricKey.BLOCK_RW_LOCK_POOL_REMAINING_RESOURCES_KEY.toString(),
                this::getRemainingPoolResources
        );
    }

    @VisibleForTesting
    public long getRemainingPoolResources() {
        return mRemainingPoolResources.get();
    }

    private void maybeLogPoolReachesFullCapacity() {
        if (this.size() == mMaxCapacity &&
                !mHasReachedFullCapacity.get() &&
                mHasReachedFullCapacity.compareAndSet(false, true)
        ) {
            LOG.warn("Block lock manager client RW lock pool exhausted. Capacity size {}", mMaxCapacity);
        }
    }

    @Override
    public ClientRWLock acquire() {
        return this.acquire(WAIT_INDEFINITELY, null);
    }

    @Nullable
    @Override
    public ClientRWLock acquire(long time, TimeUnit unit) {
        ClientRWLock ret = super.acquire(time, unit);
        maybeLogPoolReachesFullCapacity();
        if (ret != null) {
            mRemainingPoolResources.decrementAndGet();
        }
        return ret;
    }

    @Override
    public void release(ClientRWLock resource) {
        mRemainingPoolResources.incrementAndGet();
        super.release(resource);
    }

    @Override
    public void close() throws IOException {}

    @Override
    public ClientRWLock createNewResource() {
        return new ClientRWLock();
    }
}
