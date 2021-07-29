package alluxio.stress.fuse;

import alluxio.Constants;
import alluxio.stress.BaseParameters;
import alluxio.stress.GraphGenerator;
import alluxio.stress.Summary;
import alluxio.stress.TaskResult;
import alluxio.stress.common.SummaryStatistics;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.IOException;
import java.util.*;

public final class FUSEBenchTaskResult implements TaskResult, Summary {
  private long mRecordStartMs;
  private Map<Integer, ThreadCountResult> mThreadCountResults;
  private BaseParameters mBaseParameters;
  private FUSEBenchParameters mParameters;

  private Map<Integer, Map<String, SummaryStatistics>> mTimeToFirstByte;

  /**
   * Creates an instance.
   */
  public FUSEBenchTaskResult() {
    // Default constructor required for json deserialization
    mThreadCountResults = new HashMap<>();
    mTimeToFirstByte = new HashMap<>();
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  @Override
  public GraphGenerator graphGenerator() {
    return null;
  }

  private static final class Aggregator implements TaskResult.Aggregator<FUSEBenchTaskResult> {
    @Override
    public FUSEBenchTaskResult aggregate(Iterable<FUSEBenchTaskResult> results) throws Exception {
      Iterator<FUSEBenchTaskResult> it = results.iterator();
      if (it.hasNext()) {
        FUSEBenchTaskResult taskResult = it.next();
        if (it.hasNext()) {
          throw new IOException(
                  "ClientIO is a single node test, so multiple task results cannot be aggregated.");
        }
        return taskResult;
      }
      return new FUSEBenchTaskResult();
    }
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
  public FUSEBenchParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(FUSEBenchParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @return the map of thread counts to results
   */
  public Map<Integer, ThreadCountResult> getThreadCountResults() {
    return mThreadCountResults;
  }

  /**
   * @param threadCountResults the map of thread counts to results
   */
  public void setThreadCountResults(Map<Integer, ThreadCountResult> threadCountResults) {
    mThreadCountResults = threadCountResults;
  }

  /**
   * @param threadCount the thread count of the results
   * @param threadCountResult the results to add
   */
  public void addThreadCountResults(int threadCount, ThreadCountResult threadCountResult) {
    mThreadCountResults.put(threadCount, threadCountResult);
  }

  /**
   * A result for a single thread count test.
   */
  public static final class ThreadCountResult {
    private long mRecordStartMs;
    private long mEndMs;
    private long mIOBytes;
    private List<String> mErrors;

    /**
     * Creates an instance.
     */
    public ThreadCountResult() {
      // Default constructor required for json deserialization
      mErrors = new ArrayList<>();
    }

    /**
     * Merges (updates) a result with this result.
     *
     * @param result  the result to merge
     */
    public void merge(FUSEBenchTaskResult.ThreadCountResult result) {
      mRecordStartMs = Math.min(mRecordStartMs, result.mRecordStartMs);
      mEndMs = Math.max(mEndMs, result.mEndMs);
      mIOBytes += result.mIOBytes;
      mErrors.addAll(result.mErrors);
    }

    /**
     * @return the duration (in ms)
     */
    public long getDurationMs() {
      return mEndMs - mRecordStartMs;
    }

    /**
     * @param durationMs the duration (in ms)
     */
    @JsonIgnore
    public void setDurationMs(long durationMs) {
      // ignore
    }

    /**
     * Increments the bytes of IO an amount.
     *
     * @param ioBytes the amount to increment by
     */
    public void incrementIOBytes(long ioBytes) {
      mIOBytes += ioBytes;
    }


    /**
     * @param recordStartMs the start time (in ms)
     */
    public void setRecordStartMs(long recordStartMs) { mRecordStartMs = recordStartMs; }

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

    /**
     * @return the throughput (MB/s)
     */
    public float getIOMBps() {
      return ((float) mIOBytes / getDurationMs()) * 1000.0f / Constants.MB;
    }

    /**
     * @param ioMBps the throughput (MB / s)
     */
    @JsonIgnore
    public void setIOMBps(float ioMBps) {
      // ignore
    }
  }
}
