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

package alluxio.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;

import java.io.OutputStream;
import java.util.concurrent.atomic.LongAccumulator;

/**
 * A wrapper around {@link Reservoir} that keeps track of the max
 * value sent as an update to this reservoir. Any snapshot returned
 * by the reservoir will return this max value.
 */
public class MaxReservoir implements Reservoir {
  private final Reservoir mDelegate;
  private final LongAccumulator mMaxUpdate;

  /**
   * @param delegate the delegate reservoir to wrap
   */
  public MaxReservoir(Reservoir delegate) {
    mMaxUpdate = new LongAccumulator(Long::max, 0);
    mDelegate = delegate;
  }

  /**
   * Returns the number of values recorded.
   *
   * @return the number of values recorded
   */
  public int size() {
    return mDelegate.size();
  }

  /**
   * Adds a new recorded value to the reservoir.
   *
   * @param value a new recorded value
   */
  public void update(long value) {
    mMaxUpdate.accumulate(value);
    mDelegate.update(value);
  }

  /**
   * Returns a snapshot of the reservoir's values, which has the max value
   * set as the overall max value sent as an update to this reservoir.
   *
   * @return a snapshot of the reservoir's values
   */
  public Snapshot getSnapshot() {
    return new MaxSnapshot(mDelegate.getSnapshot(), mMaxUpdate.get());
  }

  private static class MaxSnapshot extends Snapshot {
    private final Snapshot mDelegate;
    private final long mMax;

    private MaxSnapshot(Snapshot delegate, long max) {
      mDelegate = delegate;
      mMax = max;
    }

    @Override
    public double getValue(double quantile) {
      return mDelegate.getValue(quantile);
    }

    @Override
    public long[] getValues() {
      return mDelegate.getValues();
    }

    @Override
    public int size() {
      return mDelegate.size();
    }

    @Override
    public long getMax() {
      return mMax;
    }

    @Override
    public double getMean() {
      return mDelegate.getMean();
    }

    @Override
    public long getMin() {
      return mDelegate.getMin();
    }

    @Override
    public double getStdDev() {
      return mDelegate.getStdDev();
    }

    @Override
    public void dump(OutputStream output) {
      mDelegate.dump(output);
    }
  }
}
