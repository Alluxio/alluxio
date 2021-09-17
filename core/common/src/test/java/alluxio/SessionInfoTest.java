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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Unit tests for {@link SessionInfo}.
 */
public final class SessionInfoTest {
  private static final int MIN_LEN = 1;
  private static final int MAX_LEN = 1000;
  private static final int DELTA = 50;
  private static final int SESSION_TIMEOUT_MS = 1000;

  /**
   * Tests the {@link SessionInfo#SessionInfo(long, int)} constructor.
   */
  @Test
  public void constructor() {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      SessionInfo tSessionInfo = new SessionInfo(k, SESSION_TIMEOUT_MS);
      assertEquals(k, tSessionInfo.getSessionId());
    }
  }

  /**
   * Tests that an exception is thrown in the {@link SessionInfo#SessionInfo(long, int)} constructor
   * when using an invalid id for the session.
   */
  @Test(expected = RuntimeException.class)
  public void constructorWithException() {
    for (int k = 0; k >= -1000; k -= DELTA) {
      SessionInfo tSessionInfo = new SessionInfo(k, SESSION_TIMEOUT_MS);
      assertEquals(k, tSessionInfo.getSessionId());
      fail("SessionId " + k + " should be invalid.");
    }
  }

  /**
   * Tests the {@link SessionInfo#getSessionId()} method.
   */
  @Test
  public void getSessionId() {
    for (int k = MIN_LEN; k < MAX_LEN; k += 66) {
      SessionInfo tSessionInfo = new SessionInfo(k, SESSION_TIMEOUT_MS);
      assertEquals(k, tSessionInfo.getSessionId());
    }
  }

  /**
   * Tests the {@link SessionInfo#timeout()} method.
   */
  @Test
  public void timeout() {
    SessionInfo tSessionInfo = new SessionInfo(1, SESSION_TIMEOUT_MS);
    assertFalse(tSessionInfo.timeout());
  }
}
