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

import java.util.LinkedList;
import java.util.List;

/**
 * Represents a time series which can be graphed in the UI.
 */
public class TimeSeries {
  private final String mName;
  private final List<DataPoint> mDataPoints;

  /**
   * Create a new time series with the given name and no data.
   * @param name name of the time series
   */
  public TimeSeries(String name) {
    mName = name;
    mDataPoints = new LinkedList<>();
  }

  /**
   * Record a value at the current time.
   * @param value value to record
   */
  public void record(double value) {
    mDataPoints.add(new DataPoint(value));
  }

  /**
   * @return the name of the time series
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the data of the time series
   */
  public List<DataPoint> getDataPoints() {
    return mDataPoints;
  }

  /**
   * Represents a datapoint in a time series. Millisecond epoch time is used for the timestamp.
   */
  public final class DataPoint {
    private final long mTimestamp;
    private final double mValue;

    /**
     * Construct a new data point with the current time as the timestamp.
     * @param value the value of the data point
     */
    public DataPoint(double value) {
      mTimestamp = System.currentTimeMillis();
      mValue = value;
    }

    /**
     * @return the timestamp of the data point in milliseconds epoch time
     */
    public long getTimeStamp() {
      return mTimestamp;
    }

    /**
     * @return the value of the data point
     */
    public double getValue() {
      return mValue;
    }
  }
}
