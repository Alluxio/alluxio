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

package alluxio.master.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;
import java.util.StringJoiner;

/**
 * Class to abstract out progress logging for journal replay.
 */
public abstract class AbstractJournalProgressLogger {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJournalProgressLogger.class);

  // could have been configurable. decided it is not really necessary.
  /** Max time to wait before actually logging. */
  public static final long MAX_LOG_INTERVAL_MS = 30_000;

  private long mLastMeasuredTime;
  private long mLastCommitIdx;
  private long mLogCount;

  private final OptionalLong mEndCommitIdx;

  /**
   * Creates a new instance of {@link AbstractJournalProgressLogger}.
   *
   * @param endComitIdx the final commit index in the journal. Used to estimate completion time
   */
  public AbstractJournalProgressLogger(OptionalLong endComitIdx) {
    mEndCommitIdx = endComitIdx;
    mLastMeasuredTime = System.currentTimeMillis();
    mLastCommitIdx = 0L;
    mLogCount = 0;
  }

  /**
   * @return the last applied commit index to a journal
   */
  public abstract long getLastAppliedIndex();

  /**
   * @return the name of the journal
   */
  public abstract String getJournalName();

  /**
   * Logs the progress of journal replay.
   *
   * This method rate limits itself on when it actually calculates and logs the message. If it is
   * called too frequently, then it will essentially be a no-op. The return value indicates whether
   * a message was logged or not as a result of calling the method.
   *
   * @return true is a message is logged, false otherwise
   */
  public boolean logProgress() {
    long now = System.currentTimeMillis();
    long nextLogTime =
        1000L * Math.min(1L << (mLogCount > 30 ? 30 : mLogCount), MAX_LOG_INTERVAL_MS);
    // Exit early if log is called too fast.
    if ((now - mLastMeasuredTime) < nextLogTime) {
      return false;
    }
    long currCommitIdx = getLastAppliedIndex();
    long timeSinceLastMeasure = (now - mLastMeasuredTime);
    long commitIdxRead = currCommitIdx - mLastCommitIdx;

    double commitIdxRateMs = ((double) commitIdxRead) / timeSinceLastMeasure;
    StringJoiner logMsg = new StringJoiner("|");
    logMsg.add(getJournalName());
    logMsg.add(String.format("current SN: %d", currCommitIdx));
    logMsg.add(String.format("entries in last %dms=%d", timeSinceLastMeasure, commitIdxRead));
    if (mEndCommitIdx.isPresent()) {
      long commitsRemaining = mEndCommitIdx.getAsLong() - currCommitIdx;
      double expectedTimeRemaining = ((double) commitsRemaining) / commitIdxRateMs;
      if (commitsRemaining > 0) {
        logMsg.add(String.format("est. commits left: %d", commitsRemaining));
      }
      if (!Double.isNaN(expectedTimeRemaining) && !Double.isInfinite(expectedTimeRemaining)
          && expectedTimeRemaining > 0) {
        logMsg.add(String.format("est. time remaining: %.2fms", expectedTimeRemaining));
      }
    }
    mLogCount++;
    LOG.info(logMsg.toString());
    mLastMeasuredTime = now;
    mLastCommitIdx = currCommitIdx;
    return true;
  }
}
