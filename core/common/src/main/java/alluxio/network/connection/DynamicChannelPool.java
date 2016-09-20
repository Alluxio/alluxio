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

// A connection can be in 3 states: ACQUIRED, CLOSED, IDLE.
abstract class LRUConnectionPool<T> {

  protected static class ConnectionInternal<T> {
    private T mConnection;

    private long mLastAccessTimeInSecs;

    public void setLastAccessTimeInSecs(long lastAccessTimeInSecs) {
      mLastAccessTimeInSecs = lastAccessTimeInSecs;
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

  public LRUConnectionPool(int maxConnections, final ExecutorService heartbeatExecutor,
      int heartbeatIntervalInSecs, int gcInternalInSecs) {
    mHeartbeatExecutor = heartbeatExecutor;
    mExecutor = new ScheduledThreadPoolExecutor(2);
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

  abstract public T acquire();
  abstract public T acquireWithTimeout();

  abstract public void release();

  abstract public void closeConnection(T connection);
  abstract public void heartbeat(T connection);
  abstract protected boolean shouldGc(ConnectionInternal<T> connectionInternal);

  protected abstract T createNewConnection();
}
