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

package alluxio.time;

import java.time.Duration;

/**
 * An interface for a utility which provides a sleep method.
 */
public interface Sleeper {

  /**
   * Sleeps for the given duration.
   *
   * @param duration the duration to sleep for
   * @throws InterruptedException if the sleep is interrupted
   */
  void sleep(Duration duration) throws InterruptedException;
}
