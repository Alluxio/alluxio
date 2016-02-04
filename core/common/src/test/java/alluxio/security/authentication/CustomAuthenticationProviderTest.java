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

package alluxio.security.authentication;

import javax.security.sasl.AuthenticationException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests the {@link CustomAuthenticationProvider} class.
 */
public class CustomAuthenticationProviderTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Tests the {@link CustomAuthenticationProvider#CustomAuthenticationProvider(String)}
   * constructor to throw an exception when the class cannot be found.
   */
  @Test
  public void classNotFoundTest() {
    String notExistClass = "alluxio.test.custom.provider";
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(notExistClass + " not found");
    new CustomAuthenticationProvider(notExistClass);
  }

  /**
   * Tests the {@link CustomAuthenticationProvider#CustomAuthenticationProvider(String)}
   * constructor to throw an exception when the class is not a provider.
   */
  @Test
  public void classNotProviderTest() {
    String notProviderClass = CustomAuthenticationProviderTest.class.getName();
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(notProviderClass + " instantiate failed :");
    new CustomAuthenticationProvider(notProviderClass);
  }

  /**
   * Tests the {@link CustomAuthenticationProvider#getCustomProvider()} method.
   */
  @Test
  public void mockCustomProviderTest() {
    CustomAuthenticationProvider provider =
        new CustomAuthenticationProvider(MockAuthenticationProvider.class.getName());
    Assert.assertTrue(provider.getCustomProvider() instanceof MockAuthenticationProvider);
  }

  /**
   * An {@link AuthenticationProvider} to use as a mock.
   */
  public static class MockAuthenticationProvider implements AuthenticationProvider {
    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      // noop
    }
  }
}
