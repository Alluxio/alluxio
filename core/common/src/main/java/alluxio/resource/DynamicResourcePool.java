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
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
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
  protected static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * A wrapper on the resource to include the last time at which it was used.
   *
   * @param <T> the resource type
   */
  protected class ResourceInternal<T> {
    /** A unique ID used to distinguish the objects. */
    private int mIdentity = System.identityHashCode(this);

    /** The resource. */
    private T mResource;

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
    public ResourceInternal(T resource) {
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

  // Tracks the resources that are available ordered by lastAccessTime (the first one is
  // the most recently used resource).
  @GuardedBy("mLock")
  private TreeSet<ResourceInternal<T>> mResourceAvailable =
      new TreeSet<>(new Comparator<ResourceInternal<T>>() {
        @Override
        public int compare(ResourceInternal<T> c1, ResourceInternal<T> c2) {
          if (c1 == c2) {
            return 0;
          }
          if (c1.mLastAccessTimeMs == c2.mLastAccessTimeMs) {
            return c1.mIdentity - c2.mIdentity;
          }
          return (int) (c2.mLastAccessTimeMs - c1.mLastAccessTimeMs);
        }
      });

  // Tracks all the resources that are not closed.
  @GuardedBy("mLock")
  private HashMap<T, ResourceInternal<T>> mResources = new HashMap<>(32);

  // Thread to scan mResourceAvailable to close those resources that are old.
  private ScheduledExecutorService mExecutor;
  private ScheduledFuture<?> mGcFuture;

  protected Clock mClock = new SystemClock();

  /**
   * Creates a dynamic pool instance.
   *
   * @param options the options
   */
  public DynamicResourcePool(Options options) {
    mExecutor = Preconditions.checkNotNull(options.getGcExecutor());

    mMaxCapacity = options.getMaxCapacity();
    mMinCapacity = options.getMinCapacity();

    mGcFuture = mExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        List<T> resourcesToGc = new ArrayList<T>();

        try {
          mLock.lock();
          if (mResources.size() <= mMinCapacity) {
            return;
          }
          int currentSize = mResources.size();
          Iterator<ResourceInternal<T>> iterator = mResourceAvailable.iterator();
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
   * @throws IOException if it fails to acquire because of the failure to create a new resource
   * @throws InterruptedException if this thread is interrupted
   */
  @Override
  public T acquire() throws IOException, InterruptedException {
    try {
      return acquire(100  /* no timeout */, TimeUnit.DAYS);
    } catch (TimeoutException e) {
      // Never should timeout in acquire().
      throw Throwables.propagate(e);
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
   * @throws IOException if it fails to acquire because of the failure to create a new resource
   * @throws TimeoutException if it fails to acquire because of time out
   * @throws InterruptedException if this thread is interrupted
   */
  @Override
  public T acquire(long time, TimeUnit unit)
      throws IOException, TimeoutException, InterruptedException {
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
        if (currTimeMs >= endTimeMs || !mNotEmpty
            .await(endTimeMs - currTimeMs, TimeUnit.MILLISECONDS)) {
          throw new TimeoutException("Acquire resource times out.");
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
   *
   * @param resource the resource to release
   */
  @Override
  public void release(T resource) {
    try {
      mLock.lock();
      if (!mResources.containsKey(resource)) {
        throw new IllegalArgumentException(
            "Resource " + resource.toString() + " was not acquired from this resource pool.");
      }
      ResourceInternal<T> resourceInternal = mResources.get(resource);
      resourceInternal.setLastAccessTimeMs(mClock.millis());
      mResourceAvailable.add(resourceInternal);
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
      if (mResourceAvailable.size() != mResources.size()) {
        LOG.warn("{} resources are not released when closing the resource pool.",
            mResources.size() - mResourceAvailable.size());
      }
      for (ResourceInternal<T> resourceInternal : mResourceAvailable) {
        closeResourceSync(resourceInternal.mResource);
      }
      mResourceAvailable.clear();
    } finally {
      mLock.unlock();
    }
    mGcFuture.cancel(true);
  }

  @Override
  public int size() {
    try {
      mLock.lock();
      return mResources.size();
    } finally {
      mLock.unlock();
    }
  }

  /**
   * @return true if the pool is full
   */
  private boolean isFull() {
    boolean full = false;
    try {
      mLock.lock();
      full = mResources.size() >= mMaxCapacity;
    } finally {
      mLock.unlock();
    }
    return full;
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
   * @return the most recently used resource
   */
  private ResourceInternal<T> poll() {
    ResourceInternal<T> resource;
    try {
      mLock.lock();
      resource = mResourceAvailable.pollFirst();
    } finally {
      mLock.unlock();
    }
    return resource;
  }

  /**
   * Check whether the resource is healthy. If not retry. When this called, the resource
   * is not in mResourceAvailable.
   *
   * @param resource the resource to check
   * @param endTimeMs the end time to wait till
   * @return the resource
   * @throws IOException if it fails to create a resource
   * @throws TimeoutException if it times out to wait for a resource
   * @throws InterruptedException if this thread is interrupted
   */
  private T checkHealthyAndRetry(T resource, long endTimeMs)
      throws IOException, TimeoutException, InterruptedException {
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
   * @throws IOException if it fails to create the resource
   */
  protected abstract T createNewResource() throws IOException;
}
