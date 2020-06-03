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

package alluxio.collections;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A wrapper class around the LinkedBlockingQueue which collects information about the amount of
 * time each item spends within the queue.
 *
 * The values reported are configurable exponential moving averages (EMA) for the given minute
 * intervals. The default constructor will report the 1, 5, and 15 minute moving averages for the
 * queue latency times.
 *
 * The EMA is calculated by the formula
 *
 * EMA(n) = \alpha(t_n - t_(n-1))*Y_n + (1-\alpha(t_n - t_(n-1)))*EMA(n-1)
 *
 * and where
 *
 *  \alpha(t_n - t_(n-1)) =  1 - exp( (t_n - t_(n-1)) / W * 6000 )
 *
 * t is measured in milliseconds and W is the number of minutes
 *
 * The intended use of this class is within ExecutorService's so that the latency of tasks can be
 * reported to users through Alluxio's metrics system.
 *
 * @param <T> the type of object to store in the queue
 */
public class LatencyReportingLinkedBlockingQueue<T> extends AbstractQueue<T>
    implements BlockingQueue<T> {

  private final EMA[] mEMAs;
  private final LinkedBlockingQueue<Pair<Long, T>> mInternalQueue;
  private final Consumer<List<EMA>> mReportingFunction;
  private final AtomicInteger mCount;
  private final int mReportAfter;

  /**
   * Create a new latency recording linked blocking queue.
   *
   * Every time an item is de-queued from this object the time spent in the queue is recorded and
   * used to update a configurable set of moving averages. The averages are passed to a reporting
   * function at a configurable frequency so that the EMAs may be set or recorded
   *
   * @param capacity the maximum capacity of this queue
   * @param reportingFunction the function to call whenever updating EMAs
   * @param reportFreq The number of de-queues between calling the reporting function
   * @param minuteAverages a list of averages (in minutes) that should be calculated
   */
  public LatencyReportingLinkedBlockingQueue(int capacity,
      Consumer<List<EMA>> reportingFunction, int reportFreq, double... minuteAverages) {
    mInternalQueue = new LinkedBlockingQueue<>(capacity);
    mEMAs = new EMA[minuteAverages.length];
    for (int i = 0; i < minuteAverages.length; i++) {
      Preconditions.checkArgument(minuteAverages[i] > 0, "minuteAverages[" + i + "] must be > 0.");
      mEMAs[i] = new EMA(minuteAverages[i]);
    }
    mReportingFunction = reportingFunction;
    mCount = new AtomicInteger(0);
    Preconditions.checkArgument(reportFreq > 0, "reportFreq should be greater than 0.");
    mReportAfter = reportFreq;
  }

  private synchronized void calculateNewEMAs(long startTime) {
    long now = System.currentTimeMillis();
    long latency = now - startTime;
    for (EMA ema : mEMAs) {
      ema.update(now, latency);
    }
    if (mCount.incrementAndGet() % mReportAfter == 0) {
      mReportingFunction.accept(Arrays.asList(mEMAs));
    }
  }

  /**
   * Return the set of EMAs in an unmodifiable list.
   *
   * The ordering of the EMAs corresponds to the order in which they were passed into the
   * constructor.
   *
   * @return the list of EMAs
   */
  public List<EMA> getEMAs() {
    return Collections.unmodifiableList(Arrays.asList(mEMAs));
  }

  @Override
  public void put(@Nonnull T t) throws InterruptedException {
    mInternalQueue.put(new Pair<>(System.currentTimeMillis(), t));
  }

  @Override
  public boolean offer(@Nonnull T t) {
    return mInternalQueue.offer(new Pair<>(System.currentTimeMillis(), t));
  }

  @Override
  public boolean offer(T t, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
    return mInternalQueue.offer(new Pair<>(System.currentTimeMillis(), t), timeout, unit);
  }

  @Override
  @Nonnull
  public T take() throws InterruptedException {
    Pair<Long, T> item = mInternalQueue.take();
    calculateNewEMAs(item.getFirst());
    return item.getSecond();
  }

  @Override
  public int remainingCapacity() {
    return mInternalQueue.remainingCapacity();
  }

  @Override
  public int drainTo(@Nonnull Collection<? super T> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    Deque<Pair<Long, T>> l = new LinkedList<>();
    int ret = mInternalQueue.drainTo(l, maxElements);
    long avgStartTime = Math.round(
        ((double) l.stream().map(Pair::getFirst).reduce(0L, Long::sum)) / l.size());
    l.stream().map(Pair::getSecond).forEach(c::add);
    calculateNewEMAs(avgStartTime);
    return ret;
  }

  @Override
  public T poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
    Pair<Long, T> item = mInternalQueue.poll(timeout, unit);
    if (item == null) {
      return null;
    }
    calculateNewEMAs(item.getFirst());
    return item.getSecond();
  }

  @Override
  public T poll() {
    Pair<Long, T> item = mInternalQueue.poll();
    if (item == null) {
      return null;
    }
    calculateNewEMAs(item.getFirst());
    return item.getSecond();
  }

  @Override
  public T peek() {
    Pair<Long, T> item = mInternalQueue.peek();
    return item == null ? null : item.getSecond();
  }

  @Override
  @Nonnull
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private final Iterator<Pair<Long, T>> mIter = mInternalQueue.iterator();
      Pair<Long, T> mLastItem = null;
      @Override
      public boolean hasNext() {
        return mIter.hasNext();
      }

      @Override
      public T next() {
        mLastItem = mIter.next();
        return mLastItem.getSecond();
      }

      @Override
      public void remove() {
        // This will occur is #next() is not called on the iterator first
        if (mLastItem != null) {
          calculateNewEMAs(mLastItem.getFirst());
        }
        mIter.remove();
      }

      @Override
      public void forEachRemaining(Consumer<? super T> action) {
        mIter.forEachRemaining((p) -> action.accept(p.getSecond()));
      }
    };
  }

  @Override
  public int size() {
    return mInternalQueue.size();
  }

  /**
   * This class is a light representation of a moving average.
   */
  public static class EMA {
    /**
     * The interval that this EMA represents. i.e. 1 minute, 30 seconds, etc.
     */
    private final double mTimeInterval;

    /**
     * The value representing the moving average.
     */
    private volatile double mEMA;

    /**
     * A value representing the last time this EMA was updated.
     */
    private final AtomicLong mLastUpdateTime = new AtomicLong(System.currentTimeMillis());

    /**
     * @param timeInterval the time interval in minutes which this EMA represents
     */
    public EMA(double timeInterval) {
      mTimeInterval = timeInterval;
      mEMA = 0;
    }

    /**
     * Update the EMA based on a given update time and a measured value for the EMA.
     *
     * @param updateTime the time the value was recorded (derived from
     *                   {@link System#currentTimeMillis()})
     * @param measuredValue the measured value to include in the EMA
     */
    synchronized void update(long updateTime, double measuredValue) {
      long timeDiff = updateTime - mLastUpdateTime.getAndSet(updateTime);
      double alpha = 1 - Math.exp(((double) -timeDiff) / (mTimeInterval * 60000));
      mEMA = (alpha * measuredValue) + ((1 - alpha) * mEMA);
    }

    /**
     * @return the last computed EMA
     */
    public double get() {
      return mEMA;
    }

    @Override
    public String toString() {
      return String.format("%.2f minute moving average: %.2f", mTimeInterval, mEMA);
    }
  }
}
