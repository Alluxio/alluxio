package alluxio.cross.cluster.cli;

import alluxio.Constants;

/**
 * Tracks success and failures for the random read client.
 */
public class RandResult {
  public int mSuccess;
  public int mFailures;
  public long mDurationMs;
  public double mOpsPerSecond;

  /**
   * @param durationMs set the duration in milliseconds
   */
  public void setDuration(long durationMs) {
    mDurationMs = durationMs;
    mOpsPerSecond = (double) (mSuccess + mFailures) / ((double) mDurationMs / Constants.SECOND_MS);
  }
}

