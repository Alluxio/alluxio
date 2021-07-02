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

package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Used to define state-lock options for taking it exclusively.
 * A typical framework for obtaining the state-lock exclusively is:
 *  1- Run a grace-cycle:
 *      cycle of {@link Lock#tryLock()} - {@link Thread#sleep(long)} calls.
 *  2- Call {@link Lock#tryLock(long, TimeUnit)} for a long duration
 *    a- Duration configured by {@link PropertyKey#MASTER_BACKUP_STATE_LOCK_FORCED_DURATION}.
 *    b- Shared lock holders/waiter will regularly be interrupted if
 *       {@link PropertyKey#MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_ENABLED} is true.
 *       - Interrupt interval defined by
 *         {@link PropertyKey#MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_INTERVAL}.
 */
public class StateLockOptions {
  /** Whether to wait grace-cycle. */
  private final GraceMode mGraceMode;

  /** try part of grace-cycle. */
  private final long mGraceCycleTryMs;
  /** sleep of grace-cycle. */
  private final long mGraceCycleSleepMs;
  /** total duration of grace-cycle. */
  private final long mGraceCycleTimeoutMs;

  /**
   * Creates an option class that is consulted while taking a state-lock exclusively
   * from {@link StateLockManager}.
   *
   * @param graceMode the mode for grace-cycle
   * @param graceCycleTryMs grace-cycle try duration
   * @param graceCycleSleepMs grace-cycle sleep duration
   * @param graceCycleTimeoutMs total grace-cycle duration
   */
  public StateLockOptions(GraceMode graceMode, long graceCycleTryMs, long graceCycleSleepMs,
      long graceCycleTimeoutMs) {
    mGraceMode = graceMode;
    mGraceCycleTryMs = graceCycleTryMs;
    mGraceCycleSleepMs = graceCycleSleepMs;
    mGraceCycleTimeoutMs = graceCycleTimeoutMs;
  }

  /**
   * @return the {@link GraceMode} of this options
   */
  public GraceMode getGraceMode() {
    return mGraceMode;
  }

  /**
   * @return try duration of grace-cycle
   */
  public long getGraceCycleTryMs() {
    return mGraceCycleTryMs;
  }

  /**
   * @return sleep duration of grace-cycle
   */
  public long getGraceCycleSleepMs() {
    return mGraceCycleSleepMs;
  }

  /**
   * @return total duration of grace-cycle
   */
  public long getGraceCycleTimeoutMs() {
    return mGraceCycleTimeoutMs;
  }

  /**
   * @return {@link StateLockOptions} default instance for shell backups
   */
  public static StateLockOptions defaultsForShellBackup() {
    return new StateLockOptions(
        ServerConfiguration.getEnum(
            PropertyKey.MASTER_SHELL_BACKUP_STATE_LOCK_GRACE_MODE, GraceMode.class),
        ServerConfiguration.getMs(
            PropertyKey.MASTER_SHELL_BACKUP_STATE_LOCK_TRY_DURATION),
        ServerConfiguration.getMs(
            PropertyKey.MASTER_SHELL_BACKUP_STATE_LOCK_SLEEP_DURATION),
        ServerConfiguration.getMs(
            PropertyKey.MASTER_SHELL_BACKUP_STATE_LOCK_TIMEOUT)
    );
  }

  /**
   * @return {@link StateLockOptions} default instance for daily backups
   */
  public static StateLockOptions defaultsForDailyBackup() {
    return new StateLockOptions(
        ServerConfiguration.getEnum(
            PropertyKey.MASTER_DAILY_BACKUP_STATE_LOCK_GRACE_MODE, GraceMode.class),
        ServerConfiguration.getMs(
            PropertyKey.MASTER_DAILY_BACKUP_STATE_LOCK_TRY_DURATION),
        ServerConfiguration.getMs(
            PropertyKey.MASTER_DAILY_BACKUP_STATE_LOCK_SLEEP_DURATION),
        ServerConfiguration.getMs(
            PropertyKey.MASTER_DAILY_BACKUP_STATE_LOCK_TIMEOUT)
    );
  }

  /**
   * This default instance is effectively the same as locking on write-lock.
   *
   * @return {@link StateLockOptions} default
   */
  public static StateLockOptions defaults() {
    return new StateLockOptions(
        GraceMode.FORCED, // force the lock
        0, // grace-cycle try duration ms
        0, // grace-cycle sleep duration ms
        0 // grace-cycle total duration ms
    );
  }

  /**
   * Defines the grace mode of exclusive locking of the state-lock.
   */
  public enum GraceMode {
    TIMEOUT,    // Timeout if lock can't be acquired by grace-cycle.
    FORCED,     // Force the lock if grace-cycle failed to acquire it.
  }
}
