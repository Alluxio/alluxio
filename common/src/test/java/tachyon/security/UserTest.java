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

package tachyon.security;

import javax.security.auth.Subject;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link tachyon.security.User}
 */
public final class UserTest {

  /**
   * This test verifies whether the {@link tachyon.security.User} could be used in Java security
   * framework.
   * @throws Exception
   */
  @Test
  public void usedInSecurityContextTest() throws Exception {
    // add new users into Subject
    Subject subject = new Subject();
    subject.getPrincipals().add(new User("realUser"));
    subject.getPrincipals().add(new User("proxyUser"));

    // fetch added users
    User[] users = subject.getPrincipals(User.class).toArray(new User[0]);

    // verification
    Assert.assertEquals(2, users.length);
    Assert.assertNotEquals(users[0], users[1]);
  }
}
