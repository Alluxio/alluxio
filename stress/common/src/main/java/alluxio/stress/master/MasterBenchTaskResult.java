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

package alluxio.stress.master;

import alluxio.Constants;
import alluxio.stress.BaseParameters;
import alluxio.stress.TaskResult;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

/**
 * The task result for the master stress tests.
 */
public final class MasterBenchTaskResult implements TaskResult {
  /** The response time histogram can record values up to this amount. */
  public static final long RESPONSE_TIME_HISTOGRAM_MAX = Constants.SECOND_NANO * 60 * 30;
  public static final int RESPONSE_TIME_HISTOGRAM_PRECISION = 3;
  public static final int MAX_RESPONSE_TIME_COUNT = 20;

  private static final int COMPRESSION_LEVEL = 9;
  private static final int RESPONSE_TIME_99_COUNT = 6;

  private long mRecordStartMs;
  private long mEndMs;
  private long mDurationMs;
  private long mNumSuccess;
  private BaseParameters mBaseParameters;
  private MasterBenchParameters mParameters;
  private List<String> mErrors;
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  private byte[] mResponseTimeNsRaw;
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  private long[] mMaxResponseTimeNs;

  /**
   * Creates an instance.
   */
  public MasterBenchTaskResult() {
    // Default constructor required for json deserialization
    mMaxResponseTimeNs = new long[MAX_RESPONSE_TIME_COUNT];
    Arrays.fill(mMaxResponseTimeNs, -1);
    mErrors = new ArrayList<>();
  }

  /**
   * Merges (updates) a task result with this result.
   *
   * @param result  the task result to merge
   */
  public void merge(MasterBenchTaskResult result) throws Exception {
    mRecordStartMs = result.mRecordStartMs;
    if (result.mEndMs > mEndMs) {
      mEndMs = result.mEndMs;
    }
    mNumSuccess += result.mNumSuccess;

    Histogram responseTime = new Histogram(RESPONSE_TIME_HISTOGRAM_MAX,
        RESPONSE_TIME_HISTOGRAM_PRECISION);
    if (mResponseTimeNsRaw != null) {
      responseTime.add(Histogram
          .decodeFromCompressedByteBuffer(ByteBuffer.wrap(mResponseTimeNsRaw),
              RESPONSE_TIME_HISTOGRAM_MAX));
    }
    if (result.mResponseTimeNsRaw != null) {
      responseTime.add(Histogram
          .decodeFromCompressedByteBuffer(ByteBuffer.wrap(result.mResponseTimeNsRaw),
              RESPONSE_TIME_HISTOGRAM_MAX));
    }
    encodeResponseTimeNsRaw(responseTime);
    mBaseParameters = result.mBaseParameters;
    mParameters = result.mParameters;
    for (int i = 0; i < mMaxResponseTimeNs.length; i++) {
      if (result.mMaxResponseTimeNs[i] > mMaxResponseTimeNs[i]) {
        mMaxResponseTimeNs[i] = result.mMaxResponseTimeNs[i];
      }
    }
    mErrors.addAll(result.mErrors);
  }

  /**
   * @return the duration (in ms)
   */
  public long getDurationMs() {
    return mDurationMs;
  }

  /**
   * @param durationMs the duration (in ms)
   */
  public void setDurationMs(long durationMs) {
    mDurationMs = durationMs;
  }

  /**
   * @return number of successes
   */
  public long getNumSuccess() {
    return mNumSuccess;
  }

  /**
   * Increments the number of successes by an amount.
   *
   * @param numSuccess the amount to increment by
   */
  public void incrementNumSuccess(long numSuccess) {
    mNumSuccess += numSuccess;
  }

  /**
   * @param numSuccess number of successes
   */
  public void setNumSuccess(long numSuccess) {
    mNumSuccess = numSuccess;
  }

  /**
   * @return the raw response time data
   */
  public byte[] getResponseTimeNsRaw() {
    return mResponseTimeNsRaw;
  }

  /**
   * Encodes the histogram into the internal byte array.
   *
   * @param responseTimeNs the histogram (in ns)
   */
  public void encodeResponseTimeNsRaw(Histogram responseTimeNs) {
    ByteBuffer bb = ByteBuffer.allocate(responseTimeNs.getEstimatedFootprintInBytes());
    responseTimeNs.encodeIntoCompressedByteBuffer(bb, COMPRESSION_LEVEL);
    bb.flip();
    mResponseTimeNsRaw = new byte[bb.limit()];
    bb.get(mResponseTimeNsRaw);
  }

  /**
   * @param responseTimeNsRaw the raw response time data
   */
  public void setResponseTimeNsRaw(byte[] responseTimeNsRaw) {
    mResponseTimeNsRaw = responseTimeNsRaw;
  }

  /**
   * @return the base parameters
   */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the base parameters
   */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the parameters
   */
  public MasterBenchParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(MasterBenchParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @return the array of max response times (in ns)
   */
  public long[] getMaxResponseTimeNs() {
    return mMaxResponseTimeNs;
  }

  /**
   * @param maxResponseTimeNs the array of max response times (in ns)
   */
  public void setMaxResponseTimeNs(long[] maxResponseTimeNs) {
    mMaxResponseTimeNs = maxResponseTimeNs;
  }

  /**
   * @return the start time (in ms)
   */
  public long getRecordStartMs() {
    return mRecordStartMs;
  }

  /**
   * @param recordStartMs the start time (in ms)
   */
  public void setRecordStartMs(long recordStartMs) {
    mRecordStartMs = recordStartMs;
  }

  /**
   * @return the end time (in ms)
   */
  public long getEndMs() {
    return mEndMs;
  }

  /**
   * @param endMs the end time (in ms)
   */
  public void setEndMs(long endMs) {
    mEndMs = endMs;
  }

  /**
   * @return the list of errors
   */
  public List<String> getErrors() {
    return mErrors;
  }

  /**
   * @param errors the list of errors
   */
  public void setErrors(List<String> errors) {
    mErrors = errors;
  }

  /**
   * @param errMesssage the error message to add
   */
  public void addErrorMessage(String errMesssage) {
    mErrors.add(errMesssage);
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator {
    @Override
    public MasterBenchSummary aggregate(Iterable<TaskResult> results) throws Exception {
      long durationMs = 0;
      long numSuccess = 0;
      long endTimeMs = 0;
      float[] maxResponseTimesMs = new float[MAX_RESPONSE_TIME_COUNT];
      Arrays.fill(maxResponseTimesMs, -1);
      MasterBenchParameters parameters = new MasterBenchParameters();
      List<String> nodes = new ArrayList<>();
      Map<String, List<String>> errors = new HashMap<>();

      Histogram responseTime = new Histogram(RESPONSE_TIME_HISTOGRAM_MAX,
          RESPONSE_TIME_HISTOGRAM_PRECISION);
      for (TaskResult taskResult : results) {
        if (!(taskResult instanceof MasterBenchTaskResult)) {
          throw new IOException(
              "TaskResult is not of type MasterBenchTaskResult. class: " + taskResult.getClass()
                  .getName());
        }
        MasterBenchTaskResult result = (MasterBenchTaskResult) taskResult;
        durationMs = Math.max(result.getEndMs() - result.getRecordStartMs(), durationMs);
        numSuccess += result.getNumSuccess();
        parameters = result.getParameters();
        if (result.getEndMs() > endTimeMs) {
          endTimeMs = result.getEndMs();
        }
        nodes.add(result.getBaseParameters().mId);
        if (!result.getErrors().isEmpty()) {
          List<String> errorList = new ArrayList<>(result.getErrors());
          errors.put(result.getBaseParameters().mId, errorList);
        }

        try {
          responseTime.add(Histogram
              .decodeFromCompressedByteBuffer(ByteBuffer.wrap(result.mResponseTimeNsRaw),
                  RESPONSE_TIME_HISTOGRAM_MAX));
        } catch (DataFormatException e) {
          throw new IOException(String.format("Failed to decode response time bytes from %s",
              result.getBaseParameters().mId), e);
        }

        for (int i = 0; i < result.getMaxResponseTimeNs().length; i++) {
          float ms = (float) result.getMaxResponseTimeNs()[i] / Constants.MS_NANO;
          if (ms > maxResponseTimesMs[i]) {
            maxResponseTimesMs[i] = ms;
          }
        }
      }

      float[] responseTimePercentile = new float[101];
      for (int i = 0; i <= 100; i++) {
        responseTimePercentile[i] =
            (float) responseTime.getValueAtPercentile(i) / Constants.MS_NANO;
      }

      float[] responseTime99Percentile = new float[RESPONSE_TIME_99_COUNT];
      for (int i = 0; i < responseTime99Percentile.length; i++) {
        responseTime99Percentile[i] =
            (float) responseTime.getValueAtPercentile(100.0 - 1.0 / (Math.pow(10.0, i)))
                / Constants.MS_NANO;
      }
      return new MasterBenchSummary(durationMs, numSuccess, endTimeMs, responseTimePercentile,
          responseTime99Percentile, maxResponseTimesMs, parameters, nodes, errors);
    }
  }
}
