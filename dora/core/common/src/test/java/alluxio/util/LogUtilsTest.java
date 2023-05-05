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

package alluxio.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests {@link LogUtils}.
 */
public final class LogUtilsTest {
  @Test
  public void truncateShort() throws Exception {
    String s;
    assertEquals("null", LogUtils.truncateMessageLineLength(null));
    s = CommonUtils.randomAlphaNumString(1);
    assertEquals(s, LogUtils.truncateMessageLineLength(s));
    s = CommonUtils.randomAlphaNumString(LogUtils.MAX_TRUNCATED_LENGTH);
    assertEquals(s, LogUtils.truncateMessageLineLength(s));

    for (int i = 0; i < 20; i++) {
      s = CommonUtils.randomAlphaNumString(
          ThreadLocalRandom.current().nextInt(2, LogUtils.MAX_TRUNCATED_LENGTH));
      assertEquals(s, LogUtils.truncateMessageLineLength(s));
    }
  }

  @Test
  public void truncateShortLines() throws Exception {
    for (int i = 0; i < 20; i++) {
      String s = "";
      for (int lines = 0; lines < ThreadLocalRandom.current().nextInt(5, 10); lines++) {
        s += "\n";
        s += CommonUtils.randomAlphaNumString(
            ThreadLocalRandom.current().nextInt(1, LogUtils.MAX_TRUNCATED_LENGTH + 1));
      }
      assertEquals(s, LogUtils.truncateMessageLineLength(s));
    }
  }

  @Test
  public void truncateLong() throws Exception {
    String s;

    for (int i = 0; i < 20; i++) {
      s = CommonUtils.randomAlphaNumString(ThreadLocalRandom.current()
          .nextInt(LogUtils.MAX_TRUNCATED_LENGTH + 1, 2 + LogUtils.MAX_TRUNCATED_LENGTH));
      String truncated = LogUtils.truncateMessageLineLength(s);
      assertTrue(truncated.startsWith(s.substring(0, LogUtils.MAX_TRUNCATED_LENGTH) + " ..."));
    }
  }

  @Test
  public void truncateLongLines() throws Exception {
    for (int i = 0; i < 20; i++) {
      String s = "";
      // generate lines both short and long
      int lines = ThreadLocalRandom.current().nextInt(5, 10);
      for (int j = 0; j < lines; j++) {
        s += "\n";
        if (j % 2 == 0) {
          s += CommonUtils.randomAlphaNumString(
              ThreadLocalRandom.current().nextInt(1, LogUtils.MAX_TRUNCATED_LENGTH + 1));
        } else {
          s = CommonUtils.randomAlphaNumString(ThreadLocalRandom.current()
              .nextInt(LogUtils.MAX_TRUNCATED_LENGTH + 1, 2 + LogUtils.MAX_TRUNCATED_LENGTH));
        }
      }

      String truncated = LogUtils.truncateMessageLineLength(s);

      String[] expectedLines = s.split("\n");
      String[] actualLines = truncated.split("\n");
      assertEquals(expectedLines.length, actualLines.length);

      // check each line
      for (int j = 0; j < expectedLines.length; j++) {
        if (expectedLines[j].length() <= LogUtils.MAX_TRUNCATED_LENGTH) {
          assertEquals(expectedLines[j], actualLines[j]);
        } else {
          assertTrue(actualLines[j]
              .startsWith(expectedLines[j].substring(0, LogUtils.MAX_TRUNCATED_LENGTH) + " ..."));
        }
      }
    }
  }

  @Test
  public void truncateSpecificLength() throws Exception {
    String s = CommonUtils.randomAlphaNumString(LogUtils.MAX_TRUNCATED_LENGTH);

    for (int length = 1; length < LogUtils.MAX_TRUNCATED_LENGTH; length++) {
      String truncated = LogUtils.truncateMessageLineLength(s, length);
      assertTrue(truncated.startsWith(s.substring(0, length) + " ..."));
    }
  }
}
