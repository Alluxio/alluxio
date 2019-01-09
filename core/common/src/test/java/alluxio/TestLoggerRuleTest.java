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

package alluxio;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link TestLoggerRule}.
 */
public final class TestLoggerRuleTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestLoggerRuleTest.class);

  @Rule
  public TestLoggerRule mLogger = new TestLoggerRule();

  @Test
  public void wasLoggedTest() {
    String logEvent = "This is a test";
    LOG.info(logEvent);
    assertTrue(mLogger.wasLogged(logEvent));
  }

  @Test
  public void logCountTest() {
    String logEvent = "I'm a test";
    assertTrue(mLogger.logCount(logEvent) == 0);
    LOG.info(logEvent);
    assertTrue(mLogger.logCount(logEvent) == 1);
    LOG.info(logEvent);
    assertTrue(mLogger.logCount(logEvent) == 2);
  }
}
