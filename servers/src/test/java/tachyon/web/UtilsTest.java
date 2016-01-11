/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.web;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Utils}.
 */
public class UtilsTest {
  @Test
  public void convertToClockTimeWithShortValue() {
    String out = Utils.convertMsToClockTime(10);
    Assert.assertEquals("0 day(s), 0 hour(s), 0 minute(s), and 0 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneSecond() {
    String out = Utils.convertMsToClockTime(TimeUnit.SECONDS.toMillis(1));
    Assert.assertEquals("0 day(s), 0 hour(s), 0 minute(s), and 1 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneMinute() {
    String out = Utils.convertMsToClockTime(TimeUnit.MINUTES.toMillis(1));
    Assert.assertEquals("0 day(s), 0 hour(s), 1 minute(s), and 0 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneMinute30Seconds() {
    String out =
        Utils.convertMsToClockTime(TimeUnit.MINUTES.toMillis(1)
            + TimeUnit.SECONDS.toMillis(30));
    Assert.assertEquals("0 day(s), 0 hour(s), 1 minute(s), and 30 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneHour() {
    String out = Utils.convertMsToClockTime(TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals("0 day(s), 1 hour(s), 0 minute(s), and 0 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneHour10Minutes45Seconds() {
    String out =
        Utils.convertMsToClockTime(TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(10)
            + TimeUnit.SECONDS.toMillis(45));
    Assert.assertEquals("0 day(s), 1 hour(s), 10 minute(s), and 45 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneDay() {
    String out = Utils.convertMsToClockTime(TimeUnit.DAYS.toMillis(1));
    Assert.assertEquals("1 day(s), 0 hour(s), 0 minute(s), and 0 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneDay4Hours10Minutes45Seconds() {
    long time =
        TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(4) + TimeUnit.MINUTES.toMillis(10)
            + TimeUnit.SECONDS.toMillis(45);
    String out = Utils.convertMsToClockTime(time);
    Assert.assertEquals("1 day(s), 4 hour(s), 10 minute(s), and 45 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneDay4Hours10Minutes45SecondsWithStopwatch() {
    long time =
        TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(4) + TimeUnit.MINUTES.toMillis(10)
            + TimeUnit.SECONDS.toMillis(45);
    String out = Utils.convertMsToClockTime(time);
    Assert.assertEquals("1 day(s), 4 hour(s), 10 minute(s), and 45 second(s)", out);
  }

  @Test(expected = IllegalArgumentException.class)
  public void convertToClockTimeWithNegativeValue() {
    Utils.convertMsToClockTime(1 - TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(4)
        + TimeUnit.MINUTES.toMillis(10) + TimeUnit.SECONDS.toMillis(45));
  }
}
