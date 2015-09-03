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
 * Unit tests for tachyon.UserInfo
 */
public class UserInfoTest {
  private static final int MIN_LEN = 1;
  private static final int MAX_LEN = 1000;
  private static final int DELTA = 50;

  private static final String USERINFOR_TOSTRING = "UserInfo( mUserId: 99, mLastHeartbeatMs: ";

  private int mUserTimeoutMs;

  @Before
  public final void before() {
    TachyonConf tachyonConf = new TachyonConf();
    mUserTimeoutMs = tachyonConf.getInt(Constants.WORKER_USER_TIMEOUT_MS);
  }

  @Test
  public void constructorTest() {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      UserInfo tUserInfo = new UserInfo(k, mUserTimeoutMs);
      Assert.assertEquals(k, tUserInfo.getUserId());
    }
  }

  @Test(expected = RuntimeException.class)
  public void constructorWithExceptionTest() {
    for (int k = 0; k >= -1000; k -= DELTA) {
      UserInfo tUserInfo = new UserInfo(k, mUserTimeoutMs);
      Assert.assertEquals(k, tUserInfo.getUserId());
      Assert.fail("UserId " + k + " should be invalid.");
    }
  }

  @Test
  public void getUserIdTest() {
    for (int k = MIN_LEN; k < MAX_LEN; k += 66) {
      UserInfo tUserInfo = new UserInfo(k, mUserTimeoutMs);
      Assert.assertEquals(k, tUserInfo.getUserId());
    }
  }

  @Test
  public void timeoutTest() {
    UserInfo tUserInfo = new UserInfo(1, mUserTimeoutMs);
    Assert.assertFalse(tUserInfo.timeout());
  }

  @Test
  public void toStringTest() {
    UserInfo tUserInfo = new UserInfo(99, mUserTimeoutMs);
    Assert.assertEquals(USERINFOR_TOSTRING,
        tUserInfo.toString().substring(0, USERINFOR_TOSTRING.length()));
  }
}
