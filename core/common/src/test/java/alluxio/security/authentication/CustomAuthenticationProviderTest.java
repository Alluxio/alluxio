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

package alluxio.security.authentication;

import static org.junit.Assert.assertTrue;

import alluxio.security.authentication.plain.CustomAuthenticationProvider;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.security.sasl.AuthenticationException;

/**
 * Tests the {@link CustomAuthenticationProvider} class.
 */
public final class CustomAuthenticationProviderTest {

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
  public void classNotFound() {
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
  public void classNotProvider() {
    String notProviderClass = CustomAuthenticationProviderTest.class.getName();
    mThrown.expect(RuntimeException.class);
    // Java 11 will add "class" prefix before the class names
    // the following messages support both java 8 and java 11
    mThrown.expectMessage("alluxio.security.authentication.CustomAuthenticationProviderTest "
        + "cannot be cast to ");
    mThrown.expectMessage("alluxio.security.authentication.AuthenticationProvider");
    new CustomAuthenticationProvider(notProviderClass);
  }

  /**
   * Tests the {@link CustomAuthenticationProvider#getCustomProvider()} method.
   */
  @Test
  public void mockCustomProvider() {
    CustomAuthenticationProvider provider =
        new CustomAuthenticationProvider(MockAuthenticationProvider.class.getName());
    assertTrue(provider.getCustomProvider() instanceof MockAuthenticationProvider);
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
