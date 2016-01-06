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

package tachyon;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.conf.TachyonConf;

/**
 * Unit tests for {@link SessionInfo}.
 */
public final class SessionInfoTest {
  private static final int MIN_LEN = 1;
  private static final int MAX_LEN = 1000;
  private static final int DELTA = 50;

  private static final String SESSIONINFOR_TOSTRING =
      "SessionInfo( mSessionId: 99, mLastHeartbeatMs: ";

  private int mSessionTimeoutMs;

  @Before
  public final void before() {
    TachyonConf tachyonConf = new TachyonConf();
    mSessionTimeoutMs = tachyonConf.getInt(Constants.WORKER_SESSION_TIMEOUT_MS);
  }

  @Test
  public void constructorTest() {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      SessionInfo tSessionInfo = new SessionInfo(k, mSessionTimeoutMs);
      Assert.assertEquals(k, tSessionInfo.getSessionId());
    }
  }

  @Test(expected = RuntimeException.class)
  public void constructorWithExceptionTest() {
    for (int k = 0; k >= -1000; k -= DELTA) {
      SessionInfo tSessionInfo = new SessionInfo(k, mSessionTimeoutMs);
      Assert.assertEquals(k, tSessionInfo.getSessionId());
      Assert.fail("SessionId " + k + " should be invalid.");
    }
  }

  @Test
  public void getSessionIdTest() {
    for (int k = MIN_LEN; k < MAX_LEN; k += 66) {
      SessionInfo tSessionInfo = new SessionInfo(k, mSessionTimeoutMs);
      Assert.assertEquals(k, tSessionInfo.getSessionId());
    }
  }

  @Test
  public void timeoutTest() {
    SessionInfo tSessionInfo = new SessionInfo(1, mSessionTimeoutMs);
    Assert.assertFalse(tSessionInfo.timeout());
  }

  @Test
  public void toStringTest() {
    SessionInfo tSessionInfo = new SessionInfo(99, mSessionTimeoutMs);
    Assert.assertEquals(SESSIONINFOR_TOSTRING,
        tSessionInfo.toString().substring(0, SESSIONINFOR_TOSTRING.length()));
  }
}
