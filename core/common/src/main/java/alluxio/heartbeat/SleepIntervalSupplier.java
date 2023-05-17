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

package alluxio.heartbeat;

/**
 * A policy to calculate the next interval to sleep.
 */
public interface SleepIntervalSupplier {
  /**
   * Gets the next interval for sleeping.
   *
   * @param previousTickedMs previous ticked time stamp in millisecond
   * @param nowTimeStampMillis current time stamp in millisecond
   * @return the interval to sleep starting from now before next time the timer triggers
   */
  long getNextInterval(long previousTickedMs, long nowTimeStampMillis);

  /**
   * Gets the run limit from previous ticked.
   *
   * @param previousTickedMs previous ticked time stamp in millisecond
   * @return the run limit
   */
  long getRunLimit(long previousTickedMs);
}
