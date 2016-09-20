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

package alluxio.network.connection;

import alluxio.Constants;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

// A connection can be in 3 states: ACQUIRED, CLOSED, IDLE.
public abstract class ConnectionPool<T> {
  protected static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  protected static class ConnectionInternal<T> {
    private T mConnection;

    private long mLastAccessTimeInSecs;

    public void setLastAccessTimeInSecs(long lastAccessTimeInSecs) {
      mLastAccessTimeInSecs = lastAccessTimeInSecs;
    }

    public ConnectionInternal(T connection) {
      connection = mConnection;
      mLastAccessTimeInSecs = System.currentTimeMillis() / Constants.SECOND_MS;
    }
  }

  // Tracks the connections that are available ordered by lastAccessTime (the first one is
  // the most recently used connection).
  private TreeSet<ConnectionInternal<T>> mConnectionAvailable = new TreeSet<>(
      new Comparator<ConnectionInternal<T>>() {
    @Override
    public int compare(ConnectionInternal<T> c1, ConnectionInternal<T> c2) {
      return (int) (c2.mLastAccessTimeInSecs - c1.mLastAccessTimeInSecs);
    }
  });

  // Tracks all the connections that are not closed.
  private ConcurrentHashMap<T, ConnectionInternal<T>> mConnections = new ConcurrentHashMap<>(32);

  // Thread to scan mConnectionAvailable to close those connections that are old.
  private ScheduledExecutorService mExecutor;
  // Executor to run the heartbeats.
  private ExecutorService mHeartbeatExecutor;

  private static final int INITIAL_DELAY = 10;

  private final ReentrantLock mTakeLock = new ReentrantLock();
  private final Condition mNotEmpty = mTakeLock.newCondition();
  private final int mMaxCapacity;
  private final AtomicInteger mCurrentCapacity = new AtomicInteger();

  public ConnectionPool(int maxConnections, final ExecutorService heartbeatExecutor,
      int heartbeatIntervalInSecs, int gcInternalInSecs) {
    mHeartbeatExecutor = heartbeatExecutor;
    mExecutor = new ScheduledThreadPoolExecutor(2);

    mMaxCapacity = maxConnections;

    mExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        List<T> connectionsToGc = new ArrayList<T>();

        synchronized (mConnectionAvailable) {
          Iterator<ConnectionInternal<T>> iterator = mConnectionAvailable.iterator();
          while (iterator.hasNext()) {
            ConnectionInternal<T> next = iterator.next();
            if (shouldGc(next)) {
              connectionsToGc.add(next.mConnection);
              iterator.remove();
              mConnections.remove(next.mConnection);
            }
          }
        }

        for (T connection : connectionsToGc) {
          closeConnection(connection);
        }
      }
    }, INITIAL_DELAY, gcInternalInSecs, TimeUnit.SECONDS);

    if (heartbeatIntervalInSecs > 0 && mHeartbeatExecutor != null) {
      mExecutor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          for (final T connection : mConnections.keySet()) {
            mHeartbeatExecutor.submit(new Runnable() {
              @Override
              public void run() {
                heartbeat(connection);
              }
            });
          }
        }
      }, INITIAL_DELAY, heartbeatIntervalInSecs, TimeUnit.SECONDS);
    }
  }

  public T acquire() {
    return acquire(null, null);
  }
  /**
   * Acquires an object of type {code T} from the pool.
   *
   * This method is like {@link #acquire()}, but it will time out if an object cannot be
   * acquired before the specified amount of time.
   *
   * @param time an amount of time to wait, null to wait indefinitely
   * @param unit the unit to use for time, null to wait indefinitely
   * @return a resource taken from the pool, or null if the operation times out
   */
  public T acquire(Integer time, TimeUnit unit) {
    // If either time or unit are null, the other should also be null.
    Preconditions.checkState((time == null) == (unit == null));
    long endTimeMs = 0;
    if (time != null) {
      endTimeMs = System.currentTimeMillis() + unit.toMillis(time);
    }

    // Try to take a resource without blocking
    synchronized (mConnectionAvailable) {
      ConnectionInternal<T> connection = mConnectionAvailable.pollFirst();
      if (connection != null) {
        if (!isHealthy(connection.mConnection)) {

          mConnections.remove(connection.mConnection);
        }
        connection.setLastAccessTimeInSecs(System.currentTimeMillis() / Constants.SECOND_MS);
        return connection.mConnection;
      }
    }

    if (mCurrentCapacity.getAndIncrement() < mMaxCapacity) {
      // If the resource pool is empty but capacity is not yet full, create a new resource.
      T connection = createNewConnection();
      ConnectionInternal<T> connectionInternal = new ConnectionInternal<>(connection);
      mConnections.put(connection, connectionInternal);
      return connection;
    }

    mCurrentCapacity.decrementAndGet();

    // Otherwise, try to take a resource from the pool, blocking if none are available.
    try {
      mTakeLock.lockInterruptibly();
      try {
        while (true) {
          ConnectionInternal<T> connection = mConnectionAvailable.pollFirst();
          if (connection != null) {
            connection.setLastAccessTimeInSecs(System.currentTimeMillis() / Constants.SECOND_MS);
            return connection.mConnection;
          }
          if (time != null) {
            long currTimeMs = System.currentTimeMillis();
            if (currTimeMs >= endTimeMs) {
              return null;
            }
            if (!mNotEmpty.await(endTimeMs - currTimeMs, TimeUnit.MILLISECONDS)) {
              return null;
            }
          } else {
            mNotEmpty.await();
          }
        }
      } finally {
        mTakeLock.unlock();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  // We always release the connection to the pool. Note that it is possible that the connection
  // is disconnected because the server is down.
  public void release(T connection) {
    if (!mConnections.contains(connection)) {
      throw new IllegalArgumentException(
          "Connection " + connection.toString() + " was not acquired from this connection pool.");
    }
    synchronized (mConnectionAvailable) {
      mConnectionAvailable.add(mConnections.get(connection));
    }
    try {
      mTakeLock.lock();
      mNotEmpty.signal();
    } finally {
      mTakeLock.unlock();
    }
  }

  public void close() {
    synchronized (mConnectionAvailable) {
      if (mConnectionAvailable.size() != mConnections.size()) {
        LOG.warn("Some connections are not released when closing the connection pool.");
      }
      for (ConnectionInternal<T> connectionInternal : mConnectionAvailable) {
        closeConnectionSync(connectionInternal.mConnection);
      }
    }
  }

  protected abstract boolean isHealthy(T connection);

  protected abstract void closeConnection(T connection);
  protected abstract void closeConnectionSync(T connection);

  protected abstract void heartbeat(T connection);
  protected abstract boolean shouldGc(ConnectionInternal<T> connectionInternal);

  protected abstract T createNewConnection();
}
