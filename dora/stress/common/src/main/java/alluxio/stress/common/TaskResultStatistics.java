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

package alluxio.stress.common;

import alluxio.Constants;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.stress.StressConstants;
import alluxio.stress.TaskResult;

import org.HdrHistogram.Histogram;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.DataFormatException;

/**
 * abstract class that calculate statistics for{@link TaskResult}.
 */
public class TaskResultStatistics {
  public long mNumSuccess;
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public byte[] mResponseTimeNsRaw;
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public long[] mMaxResponseTimeNs;

  protected TaskResultStatistics() {
    // Default constructor required for json deserialization
  }

  /**
   * Merges (updates) a task result statistics with this statistics.
   *
   * @param statistics the task result statistics to merge
   */
  public void merge(TaskResultStatistics statistics) throws Exception {
    mNumSuccess += statistics.mNumSuccess;

    Histogram responseTime =
        new Histogram(StressConstants.TIME_HISTOGRAM_MAX, StressConstants.TIME_HISTOGRAM_PRECISION);
    if (mResponseTimeNsRaw != null) {
      responseTime.add(Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(mResponseTimeNsRaw),
          StressConstants.TIME_HISTOGRAM_MAX));
    }
    if (statistics.mResponseTimeNsRaw != null) {
      responseTime.add(Histogram.decodeFromCompressedByteBuffer(
          ByteBuffer.wrap(statistics.mResponseTimeNsRaw), StressConstants.TIME_HISTOGRAM_MAX));
    }
    encodeResponseTimeNsRaw(responseTime);
    for (int i = 0; i < mMaxResponseTimeNs.length; i++) {
      if (statistics.mMaxResponseTimeNs[i] > mMaxResponseTimeNs[i]) {
        mMaxResponseTimeNs[i] = statistics.mMaxResponseTimeNs[i];
      }
    }
  }

  /**
   * Encodes the histogram into the internal byte array.
   *
   * @param responseTimeNs the histogram (in ns)
   */
  public void encodeResponseTimeNsRaw(Histogram responseTimeNs) {
    ByteBuffer bb = ByteBuffer.allocate(responseTimeNs.getEstimatedFootprintInBytes());
    responseTimeNs.encodeIntoCompressedByteBuffer(bb,
        StressConstants.TIME_HISTOGRAM_COMPRESSION_LEVEL);
    bb.flip();
    mResponseTimeNsRaw = new byte[bb.limit()];
    bb.get(mResponseTimeNsRaw);
  }

  /**
   * Converts this class to {@link SummaryStatistics}.
   *
   * @return new SummaryStatistics
   * @throws DataFormatException if histogram decoding from compressed byte buffer fails
   */
  public SummaryStatistics toBenchSummaryStatistics() throws DataFormatException {
    Histogram responseTime =
        new Histogram(StressConstants.TIME_HISTOGRAM_MAX, StressConstants.TIME_HISTOGRAM_PRECISION);
    if (mResponseTimeNsRaw != null) {
      responseTime.add(Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(mResponseTimeNsRaw),
          StressConstants.TIME_HISTOGRAM_MAX));
    }
    float[] responseTimePercentile = new float[101];
    for (int i = 0; i <= 100; i++) {
      responseTimePercentile[i] = (float) responseTime.getValueAtPercentile(i) / Constants.MS_NANO;
    }

    float[] responseTime99Percentile = new float[StressConstants.TIME_99_COUNT];
    for (int i = 0; i < responseTime99Percentile.length; i++) {
      responseTime99Percentile[i] =
          (float) responseTime.getValueAtPercentile(100.0 - 1.0 / (Math.pow(10.0, i)))
              / Constants.MS_NANO;
    }

    float[] maxResponseTimesMs = new float[mMaxResponseTimeNs.length];
    Arrays.fill(maxResponseTimesMs, -1);
    for (int i = 0; i < mMaxResponseTimeNs.length; i++) {
      maxResponseTimesMs[i] = (float) mMaxResponseTimeNs[i] / Constants.MS_NANO;
    }

    return new SummaryStatistics(mNumSuccess, responseTimePercentile, responseTime99Percentile,
        maxResponseTimesMs);
  }
}
