package alluxio.stress.master;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MasterBenchSummaryStatistics {
  /** response times for all percentiles from 0 -> 100 (101 values). */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  private float[] mResponseTimePercentileMs;
  /** percentiles of just 99.x%. first entry is 99%, second is 99.9%, etc. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  private float[] mResponseTime99PercentileMs;
  /** max response time over time, over the duration of the test. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  private float[] mMaxResponseTimeMs;



}
