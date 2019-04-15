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

package alluxio.job.util;

import alluxio.Constants;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class to record a time series, e.g. traffic over time.
 */
@NotThreadSafe
public final class TimeSeries implements Serializable {
  private static final long serialVersionUID = -9139286113871170329L;

  private final long mWidthNano;
  private TreeMap<Long, Integer> mSeries = new TreeMap<>();

  /**
   * Class contains the summary of the TimeSeries.
   */
  public class Summary {
    /**
     * Creates a {@link Summary} instance.
     */
    public Summary() {}

    public double mMean = 0;
    public double mPeak = 0;
    public double mStddev = 0;
  }

  /**
   * Creates a TimeSeries instance with given width.
   *
   * @param widthNano the granularity of the time series. If this is set to 1 min, we count
   *                  the number of events of every minute.
   */
  public TimeSeries(long widthNano) {
    mWidthNano = widthNano;
  }

  /**
   * Creates a TimeSeries instance with default width set to 1 second.
   */
  public TimeSeries() {
    mWidthNano = Constants.SECOND_NANO;
  }

  /**
   * Record one event at a timestamp into the time series.
   *
   * @param timeNano the time in nano seconds
   */
  public void record(long timeNano) {
    record(timeNano, 1);
  }

  /**
   * Record events at a timestamp into the time series.
   * @param timeNano the time in nano seconds
   * @param numEvents the number of events happened at timeNano
   */
  public void record(long timeNano, int numEvents) {
    long leftEndPoint = bucket(timeNano);
    mSeries.put(leftEndPoint, mSeries.getOrDefault(leftEndPoint, 0) + numEvents);
  }

  /**
   * @param timeNano the time in nano seconds
   * @return the number of event happened in the bucket that includes timeNano
   */
  public int get(long timeNano) {
    long leftEndPoint = bucket(timeNano);
    return mSeries.getOrDefault(leftEndPoint, 0);
  }

  /**
   * @return the width
   */
  public long getWidthNano() {
    return mWidthNano;
  }

  /**
   * @return the whole time series
   */
  public TreeMap<Long, Integer> getSeries() {
    return mSeries;
  }

  /**
   * Add one histogram to the current one. We preserve the width in the current TimeSeries.
   *
   * @param other the TimeSeries instance to add
   */
  public void add(TimeSeries other) {
    TreeMap<Long, Integer> otherSeries = other.getSeries();
    for (Map.Entry<Long, Integer> event : otherSeries.entrySet()) {
      record(event.getKey() + other.getWidthNano() / 2, event.getValue());
    }
  }

  /**
   * @return the {@link Summary}
   */
  public Summary getSummary() {
    Summary summary = new Summary();
    if (mSeries.isEmpty()) {
      return summary;
    }

    for (Integer value : mSeries.values()) {
      summary.mMean += value;
      summary.mPeak = Math.max(summary.mPeak, value);
    }
    long totalTime = (mSeries.lastKey() - mSeries.firstKey()) / mWidthNano + 1;
    summary.mMean /= totalTime;

    for (Integer value : mSeries.values()) {
      summary.mStddev += (value - summary.mMean) * (value - summary.mMean);
    }

    // Add the missing zeros.
    summary.mStddev += summary.mMean * summary.mMean * (totalTime - mSeries.size());

    summary.mStddev /= totalTime;
    summary.mStddev = Math.sqrt(summary.mStddev);

    return summary;
  }

  @Override
  public String toString() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);

    print(printStream);
    printStream.close();
    try {
      outputStream.close();
    } catch (IOException e) {
      // This should never happen.
      throw new RuntimeException(e);
    }
    return outputStream.toString();
  }

  /**
   * Print the time series sparsely, i.e. it ignores buckets with 0 events.
   *
   * @param stream the print stream
   */
  public void sparsePrint(PrintStream stream) {
    if (mSeries.isEmpty()) {
      return;
    }
    long start = mSeries.firstKey();
    stream.printf("Time series starts at %d with width %d.%n", start, mWidthNano);

    for (Map.Entry<Long, Integer> entry : mSeries.entrySet()) {
      stream.printf("%d %d%n", (entry.getKey() - start) / mWidthNano, entry.getValue());
    }
  }

  /**
   * Print the time series densely, i.e. it doesn't ignore buckets with 0 events.
   *
   * @param stream the print stream
   */
  public void print(PrintStream stream) {
    if (mSeries.isEmpty()) {
      return;
    }
    long start = mSeries.firstKey();
    stream.printf("Time series starts at %d with width %d.%n", start, mWidthNano);
    int bucketIndex = 0;
    Iterator<Map.Entry<Long, Integer>> it = mSeries.entrySet().iterator();

    Map.Entry<Long, Integer> current = it.next();
    while (current != null) {
      int numEvents = 0;
      if (bucketIndex * mWidthNano + start == current.getKey()) {
        numEvents = current.getValue();
        current = null;
        if (it.hasNext()) {
          current = it.next();
        }
      }
      stream.printf("%d %d%n", bucketIndex, numEvents);
      bucketIndex++;
    }
  }

  /**
   * @param timeNano the time in nano seconds
   * @return the bucketed timestamp in nano seconds
   */
  private long bucket(long timeNano) {
    return timeNano / mWidthNano * mWidthNano;
  }
}

