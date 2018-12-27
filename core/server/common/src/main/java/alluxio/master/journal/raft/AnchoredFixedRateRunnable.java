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

package alluxio.master.journal.raft;

import alluxio.time.Sleeper;
import alluxio.time.ThreadSleeper;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;

/**
 * Class for running a task a certain number of times each day. The times for the task to run are
 * determined by dividing the day into equal-sized intervals, then aligning the run times with an
 * anchor time. For example, if the anchor time is 4am and the task runs 3 times per day, the task
 * will run at 4am, noon, and 8pm every day. The anchor time avoids issues with clock drift and
 * rounding errors.
 */
public class AnchoredFixedRateRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(AnchoredFixedRateRunnable.class);

  private final int mTimesPerDay;
  private final LocalTime mAnchorTime;
  private final Runnable mTask;
  private final Clock mClock;
  private final Sleeper mSleeper;

  private final Duration mInterval;

  /**
   * @param anchorTime time to run the task at every day
   * @param timesPerDay number of times to run the task every day
   * @param task the task to run
   */
  public AnchoredFixedRateRunnable(LocalTime anchorTime, int timesPerDay, Runnable task) {
    this(anchorTime, timesPerDay, task, Clock.systemUTC(), ThreadSleeper.INSTANCE);
  }

  @VisibleForTesting
  AnchoredFixedRateRunnable(LocalTime anchorTime, int timesPerDay, Runnable task, Clock clock,
      Sleeper sleeper) {
    mAnchorTime = anchorTime;
    mTimesPerDay = timesPerDay;
    mTask = task;
    mClock = clock;
    mSleeper = sleeper;

    mInterval = Duration.ofDays(1).dividedBy(mTimesPerDay);
  }

  @Override
  public void run() {
    while (true) {
      try {
        mSleeper.sleep(timeToNextRun());
      } catch (InterruptedException e) {
        return;
      }
      try {
        mTask.run();
      } catch (Throwable t) {
        LOG.error("Failed to run task", t);
      }
    }
  }

  private Duration timeToNextRun() {
    LocalTime now = LocalTime.now(mClock);
    long intervalInd = timeBetween(mAnchorTime, now).toMillis() / mInterval.toMillis();
    LocalTime next = mAnchorTime.plus(mInterval.multipliedBy(intervalInd + 1));
    return timeBetween(now, next);
  }

  private Duration timeBetween(LocalTime start, LocalTime end) {
    if (start.isBefore(end)) {
      return Duration.between(start, end);
    }
    return Duration.ofDays(1).minus(Duration.between(end, start));
  }
}
