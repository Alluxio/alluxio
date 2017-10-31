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

package alluxio.resource;

import alluxio.Constants;
import alluxio.clock.Clock;
import alluxio.clock.SystemClock;

import com.google.common.base.Preconditions;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A dynamic pool that manages the resources. It clears old resources.
 * It accepts a min and max capacity.
 *
 * When acquiring resources, the most recently used resource is returned.
 *
 * @param <T> the type of the resource
 */
@ThreadSafe
public abstract class DynamicResourcePool<T> implements Pool<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicResourcePool.class);

  /**
   * A wrapper on the resource to include the last time at which it was used.
   *
   * @param <R> the resource type
   */
  protected class ResourceInternal<R> {
    /** The resource. */
    private R mResource;

    /** The last access time in ms. */
    private long mLastAccessTimeMs;

    /**
     * @param lastAccessTimeMs the last access time in ms
     */
    public void setLastAccessTimeMs(long lastAccessTimeMs) {
      mLastAccessTimeMs = lastAccessTimeMs;
    }

    /**
     * @return the last access time in ms
     */
    public long getLastAccessTimeMs() {
      return mLastAccessTimeMs;
    }

    /**
     * Creates a {@link ResourceInternal} instance.
     *
     * @param resource the resource
     */
    public ResourceInternal(R resource) {
      mResource = resource;
      mLastAccessTimeMs = mClock.millis();
    }
  }

  /**
   * Options to initialize a Dynamic resource pool.
   */
  public static final class Options {
    /** The max capacity. */
    private int mMaxCapacity = 1024;

    /** The min capacity. */
    private int mMinCapacity = 1;

    /** The initial delay. */
    private long mInitialDelayMs = 100;

    /** The gc interval. */
    private long mGcIntervalMs = 120 * Constants.SECOND_MS;

    /** The gc executor. */
    private ScheduledExecutorService mGcExecutor;

    /**
     * @return the max capacity
     */
    public int getMaxCapacity() {
      return mMaxCapacity;
    }

    /**
     * @return the min capacity
     */
    public int getMinCapacity() {
      return mMinCapacity;
    }

    /**
     * @return the initial delay
     */
    public long getInitialDelayMs() {
      return mInitialDelayMs;
    }

    /**
     * @return the gc interval
     */
    public long getGcIntervalMs() {
      return mGcIntervalMs;
    }

    /**
     * @return the gc executor
     */
    public ScheduledExecutorService getGcExecutor() {
      return mGcExecutor;
    }

    /**
     * @param maxCapacity the max capacity
     * @return the updated object
     */
    public Options setMaxCapacity(int maxCapacity) {
      Preconditions.checkArgument(maxCapacity >= 1);
      mMaxCapacity = maxCapacity;
      return this;
    }

    /**
     * @param minCapacity the min capacity
     * @return the updated object
     */
    public Options setMinCapacity(int minCapacity) {
      Preconditions.checkArgument(minCapacity >= 0);
      mMinCapacity = minCapacity;
      return this;
    }

    /**
     * @param initialDelayMs the initial delay
     * @return the updated object
     */
    public Options setInitialDelayMs(long initialDelayMs) {
      Preconditions.checkArgument(initialDelayMs >= 0);
      mInitialDelayMs = initialDelayMs;
      return this;
    }

    /**
     * @param gcIntervalMs the gc interval
     * @return the updated object
     */
    public Options setGcIntervalMs(long gcIntervalMs) {
      Preconditions.checkArgument(gcIntervalMs > 0);
      mGcIntervalMs = gcIntervalMs;
      return this;
    }

    /**
     * @param gcExecutor the gc executor
     * @return updated object
     */
    public Options setGcExecutor(ScheduledExecutorService gcExecutor) {
      mGcExecutor = gcExecutor;
      return this;
    }

    private Options() {
    }  // prevents instantiation

    /**
     * @return the default option
     */
    public static Options defaultOptions() {
      return new Options();
    }
  }

  private final ReentrantLock mLock = new ReentrantLock();
  private final Condition mNotEmpty = mLock.newCondition();

  /** The max capacity. */
  private final int mMaxCapacity;

  /** The min capacity. */
  private final int mMinCapacity;

  // Tracks the resources that are available ordered by lastAccessTime (the head is
  // the most recently used resource).
  // These are the resources that acquire() will take.
  // This is always a subset of the other data structure mResources.
  @GuardedBy("mLock")
  private final Deque<ResourceInternal<T>> mAvailableResources;

  // Tracks all the resources that are not closed.
  // put/delete operations are guarded by "mLock" so that we can control its size to be within
  // a [min, max] range. mLock is reused for simplicity. A separate lock can be used if we see
  // any performance overhead.
  private final ConcurrentHashMapV8<T, ResourceInternal<T>> mResources =
      new ConcurrentHashMapV8<>(32);

  // Thread to scan mAvailableResources to close those resources that are old.
  private ScheduledExecutorService mExecutor;
  private ScheduledFuture<?> mGcFuture;

  protected Clock mClock = new SystemClock();

  /**
   * Creates a dynamic pool instance.
   *
   * @param options the options
   */
  public DynamicResourcePool(Options options) {
    mExecutor = Preconditions.checkNotNull(options.getGcExecutor(), "executor");

    mMaxCapacity = options.getMaxCapacity();
    mMinCapacity = options.getMinCapacity();
    mAvailableResources = new ArrayDeque<>(Math.min(mMaxCapacity, 32));

    mGcFuture = mExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        List<T> resourcesToGc = new ArrayList<>();

        try {
          mLock.lock();
          if (mResources.size() <= mMinCapacity) {
            return;
          }
          int currentSize = mResources.size();
          Iterator<ResourceInternal<T>> iterator = mAvailableResources.iterator();
          while (iterator.hasNext()) {
            ResourceInternal<T> next = iterator.next();
            if (shouldGc(next)) {
              resourcesToGc.add(next.mResource);
              iterator.remove();
              mResources.remove(next.mResource);
              currentSize--;
              if (currentSize <= mMinCapacity) {
                break;
              }
            }
          }
        } finally {
          mLock.unlock();
        }

        for (T resource : resourcesToGc) {
          LOG.info("Resource {} is garbage collected.", resource);
          closeResource(resource);
        }
      }
    }, options.getInitialDelayMs(), options.getGcIntervalMs(), TimeUnit.MILLISECONDS);
  }

  /**
   * Acquire a resource of type {code T} from the pool.
   *
   * @return the acquired resource
   */
  @Override
  public T acquire() throws IOException {
    try {
      return acquire(100  /* no timeout */, TimeUnit.DAYS);
    } catch (TimeoutException e) {
      // Never should timeout in acquire().
      throw new RuntimeException(e);
    }
  }

  /**
   * Acquires a resource of type {code T} from the pool.
   *
   * This method is like {@link #acquire()}, but it will time out if an object cannot be
   * acquired before the specified amount of time.
   *
   * @param time an amount of time to wait
   * @param unit the unit to use for time
   * @return a resource taken from the pool
   * @throws TimeoutException if it fails to acquire because of time out
   */
  @Override
  public T acquire(long time, TimeUnit unit) throws TimeoutException, IOException {
    long endTimeMs = mClock.millis() + unit.toMillis(time);

    // Try to take a resource without blocking
    ResourceInternal<T> resource = poll();
    if (resource != null) {
      return checkHealthyAndRetry(resource.mResource, endTimeMs);
    }

    if (!isFull()) {
      // If the resource pool is empty but capacity is not yet full, create a new resource.
      T newResource = createNewResource();
      ResourceInternal<T> resourceInternal = new ResourceInternal<>(newResource);
      if (add(resourceInternal)) {
        return newResource;
      } else {
        closeResource(newResource);
      }
    }

    // Otherwise, try to take a resource from the pool, blocking if none are available.
    try {
      mLock.lock();
      while (true) {
        resource = poll();
        if (resource != null) {
          break;
        }
        long currTimeMs = mClock.millis();
        try {
          if (currTimeMs >= endTimeMs || !mNotEmpty
              .await(endTimeMs - currTimeMs, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Acquire resource times out.");
          }
        } catch (InterruptedException e) {
          // Restore the interrupt flag so that it can be handled later.
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      mLock.unlock();
    }

    return checkHealthyAndRetry(resource.mResource, endTimeMs);
  }

  /**
   * Releases the resource to the pool. It expects the resource to be released was acquired from
   * this pool.
   * {@link DynamicResourcePool#release(Object)} and {@link DynamicResourcePool#acquire()} must be
   * paired. Do not release the resource acquired multiple times. The behavior is undefined if
   * that happens.
   *
   * @param resource the resource to release
   */
  @Override
  public void release(T resource) {
    // We don't need to acquire mLock here because the resource is guaranteed not to be removed
    // if it is not available (i.e. not in mAvailableResources list).
    if (!mResources.containsKey(resource)) {
      throw new IllegalArgumentException(
          "Resource " + resource.toString() + " was not acquired from this resource pool.");
    }
    ResourceInternal<T> resourceInternal = mResources.get(resource);
    resourceInternal.setLastAccessTimeMs(mClock.millis());
    try {
      mLock.lock();
      mAvailableResources.addFirst(resourceInternal);
      mNotEmpty.signal();
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Closes the pool and clears all the resources. The resource pool should not be used after this.
   */
  @Override
  public void close() {
    try {
      mLock.lock();
      if (mAvailableResources.size() != mResources.size()) {
        LOG.warn("{} resources are not released when closing the resource pool.",
            mResources.size() - mAvailableResources.size());
      }
      for (ResourceInternal<T> resourceInternal : mAvailableResources) {
        closeResource(resourceInternal.mResource);
      }
      mAvailableResources.clear();
    } finally {
      mLock.unlock();
    }
    mGcFuture.cancel(true);
  }

  @Override
  public int size() {
    return mResources.size();
  }

  /**
   * @return true if the pool is full
   */
  private boolean isFull() {
    return mResources.size() >= mMaxCapacity;
  }

  /**
   * Adds a newly created resource to the pool. The resource is not available when it is added.
   *
   * @param resource
   * @return true if the resource is successfully added
   */
  private boolean add(ResourceInternal<T> resource) {
    try {
      mLock.lock();
      if (mResources.size() >= mMaxCapacity) {
        return false;
      } else {
        mResources.put(resource.mResource, resource);
        return true;
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Removes an existing resource from the pool.
   *
   * @param resource
   */
  private void remove(T resource) {
    try {
      mLock.lock();
      mResources.remove(resource);
    } finally {
      mLock.unlock();
    }
  }

  /**
   * @return the most recently used resource and null if there are no free resources
   */
  private ResourceInternal<T> poll() {
    try {
      mLock.lock();
      return mAvailableResources.pollFirst();
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Check whether the resource is healthy. If not retry. When this called, the resource
   * is not in mAvailableResources.
   *
   * @param resource the resource to check
   * @param endTimeMs the end time to wait till
   * @return the resource
   * @throws TimeoutException if it times out to wait for a resource
   */
  private T checkHealthyAndRetry(T resource, long endTimeMs) throws TimeoutException, IOException {
    if (isHealthy(resource)) {
      return resource;
    } else {
      LOG.info("Clearing unhealthy resource {}.", resource);
      remove(resource);
      closeResource(resource);
      return acquire(endTimeMs - mClock.millis(), TimeUnit.MILLISECONDS);
    }
  }

  // The following functions should be overridden by implementations.

  /**
   * @param resourceInternal the resource to check
   * @return true if the resource should be garbage collected
   */
  protected abstract boolean shouldGc(ResourceInternal<T> resourceInternal);

  /**
   * Checks whether a resource is healthy or not.
   *
   * @param resource the resource to check
   * @return true if the resource is healthy
   */
  protected abstract boolean isHealthy(T resource);

  /**
   * Closes the resource. After this, the resource should not be used. It is not guaranteed that
   * the resource is closed after the function returns.
   *
   * @param resource the resource to close
   */
  protected abstract void closeResource(T resource);

  /**
   * Similar as above but this guarantees that the resource is closed after the function returns
   * unless it fails to close.
   *
   * @param resource the resource to close
   */
  protected abstract void closeResourceSync(T resource);

  /**
   * Creates a new resource.
   *
   * @return the newly created resource
   */
  protected abstract T createNewResource() throws IOException;
}
