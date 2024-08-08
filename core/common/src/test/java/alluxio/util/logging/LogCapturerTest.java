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

package alluxio.util.logging;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link LogCapturer}.
 */
public class LogCapturerTest {
  private static final Logger LOG = LoggerFactory.getLogger(LogCapturerTest.class);

  @Test
  public void testLogCapture() {
    // Capture logs logged by LogCapturerTest
    LogCapturer log = LogCapturer.captureLogs(LoggerFactory.getLogger(LogCapturerTest.class));
    LOG.info("test logCapturer. id={}", 1);
    assertTrue(log.getOutput().contains("test logCapturer. id=1"));
    log.clearOutput();
    assertTrue(!log.getOutput().contains("test logCapturer"));
  }

  @Test
  public void testLogCapture2() {
    // Capture logs logged by LogCapturerTest
    LogCapturer log = LogCapturer.captureLogs(LogCapturerTest.LOG);
    LOG.info("test logCapturer. id={}", 2);
    assertTrue(log.getOutput().contains("test logCapturer. id=2"));
    log.clearOutput();
    assertTrue(!log.getOutput().contains("test logCapturer"));
  }
}
