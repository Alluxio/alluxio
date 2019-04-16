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
import java.util.concurrent.TimeUnit;

/**
 * A monotonically increasing clock for calculating elapsed time.
 * NOTE: this is not related to system time so the value returned cannot be used for representing
 * date time. The time can be different across JVM. Do not use it for across processes/machines
 * calculation.
 */
public final class ElapsedTimeClock extends Clock {
  @Override
  public long millis() {
    return TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
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
