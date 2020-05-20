package alluxio.stress.master;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MasterBenchSummaryStatistics {
  /** number of successes */
  public long mNumSuccess;

  /** response times for all percentiles from 0 -> 100 (101 values). */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public float[] mResponseTimePercentileMs;
  /** percentiles of just 99.x%. first entry is 99%, second is 99.9%, etc. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public float[] mResponseTime99PercentileMs;
  /** max response time over time, over the duration of the test. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public float[] mMaxResponseTimeMs;

  /**
   * Creates an instance.
   */
  public MasterBenchSummaryStatistics() {
    // Default constructor required for json deserialization
  }

  /**
   * Creates an instance.
   *
   * @param numSuccess the number of success
   * @param responseTimePercentileMs the response times (in ms), for all percentiles
   * @param responseTime99PercentileMs the response times (in ms), for the 99.x percentiles
   * @param maxResponseTimeMs the max response times (in ms) over time
   */
  public MasterBenchSummaryStatistics(long numSuccess, float[] responseTimePercentileMs,
                                      float[] responseTime99PercentileMs,
                                      float[] maxResponseTimeMs) {
    mNumSuccess = numSuccess;
    mResponseTimePercentileMs = responseTimePercentileMs;
    mResponseTime99PercentileMs = responseTime99PercentileMs;
    mMaxResponseTimeMs = maxResponseTimeMs;
  }
}
