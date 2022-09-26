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

