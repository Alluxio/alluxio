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

package alluxio.clock;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 * A clock representing the current time as reported by the operating system.
 */
public final class SystemClock extends Clock {
  /**
   * Constructs a new {@link Clock} which reports the actual time.
   */
  public SystemClock() {}

  @Override
  public long millis() {
    return System.currentTimeMillis();
  }

  @Override
  public ZoneId getZone() {
    return null;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return null;
  }

  @Override
  public Instant instant() {
    return null;
  }
}
