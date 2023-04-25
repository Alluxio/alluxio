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

package alluxio.util.executor;

import alluxio.collections.ConcurrentHashSet;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A blocking queue containing only unique elements, based on LinkedBlockingQueue implementation.
 *
 * We serialize the insertion into the queue, otherwise, we may end up with duplicate elements
 * in the queue.
 *
 * @param <T> element type
 */
public class UniqueBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {
  private ConcurrentHashSet<T> mElementSet = new ConcurrentHashSet<>();
  private BlockingQueue<T> mBlockingQueue;

  /**
   * Constructor for a UniqueBlockingQueue.
   *
   * @param capacity capacity of the blocking queue
   */
  public UniqueBlockingQueue(int capacity) {
    mBlockingQueue = new LinkedBlockingQueue<>(capacity);
  }

  @Override
  public synchronized void put(T e) throws InterruptedException {
    if (!mElementSet.contains(e)) {
      mBlockingQueue.put(e);
      mElementSet.add(e);
    }
  }

  @Override
  public synchronized boolean offer(T e) {
    // the interface description suggests that offer can only fail for capacity reason, but we
    // are failing for uniqueness reasons.
    if (mElementSet.contains(e)) {
      return false;
    }
    if (mBlockingQueue.offer(e)) {
      mElementSet.add(e);
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
    // the interface description suggests that offer can only fail for capacity reason, but we
    // are failing for uniqueness reasons.
    if (mElementSet.contains(e)) {
      return false;
    }
    if (mBlockingQueue.offer(e, timeout, unit)) {
      mElementSet.add(e);
      return true;
    }
    return false;
  }

  @Override
  public T take() throws InterruptedException {
    T e = mBlockingQueue.take();
    mElementSet.remove(e);
    return e;
  }

  @Override
  public int remainingCapacity() {
    return mBlockingQueue.remainingCapacity();
  }

  @Override
  public int drainTo(Collection<? super T> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    int numberOfElements = mBlockingQueue.drainTo(c, maxElements);
    if (numberOfElements > 0) {
      mElementSet.removeAll(c);
    }
    return numberOfElements;
  }

  @Override
  public Iterator<T> iterator() {
    Iterator<T> iter = mBlockingQueue.iterator();
    Iterator<T> it = new Iterator<T>() {
      private T mLastElem = null;
      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public T next() {
        mLastElem = iter.next();
        return mLastElem;
      }

      @Override
      public void remove() {
        iter.remove();
        if (mLastElem != null) {
          mElementSet.remove(mLastElem);
        }
        mLastElem = null;
      }
    };
    return it;
  }

  @Override
  public int size() {
    return mBlockingQueue.size();
  }

  @Override
  public T poll() {
    T e = mBlockingQueue.poll();
    if (e != null) {
      mElementSet.remove(e);
    }
    return e;
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    T e = mBlockingQueue.poll(timeout, unit);
    if (e != null) {
      mElementSet.remove(e);
    }
    return e;
  }

  @Override
  public T peek() {
    return mBlockingQueue.peek();
  }
}
