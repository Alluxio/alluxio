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

import static org.junit.Assert.assertSame;

import alluxio.conf.InstancedConfiguration;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;

import org.junit.After;
import org.junit.Test;

/**
 * Unit tests for {@link AuthenticatedClientUserResource}.
 */
public final class AuthenticatedClientUserResourceTest {
  private static final String TESTCASE_USER = "userA";
  private static final String ORIGINAL_USER = "alluxio";

  @After
  public void after() {
    AuthenticatedClientUser.remove();
  }

  @Test
  public void userRestored() throws Exception {
    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    AuthenticatedClientUser.set(ORIGINAL_USER);
    User original = AuthenticatedClientUser.get(conf);
    new AuthenticatedClientUserResource(TESTCASE_USER, conf).close();
    assertSame(original, AuthenticatedClientUser.get(conf));
  }
}
