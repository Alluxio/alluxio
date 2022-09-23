package alluxio.cross.cluster.cli;

import alluxio.Constants;
import alluxio.util.JsonSerializable;
import java.util.zip.DataFormatException;

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

  /**
   * Merge with another rand result.
   * @param other the result to merge with
   * @return the merged result
   */
  public RandResult merge(RandResult other) {
    RandResult result = new RandResult();
    result.mSuccess = mSuccess + other.mSuccess;
    result.mFailures = mFailures + other.mFailures;
    result.setDuration(Math.max(mDurationMs, other.mDurationMs));
    return result;
  }

  class RandResultsSummary implements JsonSerializable {
    public RandResult mRandomReadResults = RandResult.this;

    RandResultsSummary() {
    }
  }

  /**
   * @return the results as a summary
   */
  JsonSerializable toSummary() throws DataFormatException {
    return new RandResultsSummary();
  }
}

