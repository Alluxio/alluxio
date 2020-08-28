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

import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.authentication.plain.SaslClientHandlerPlain;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.security.auth.Subject;

/**
 * Tests {@link SaslClientHandler} and {@link SaslServerHandler} implementations.
 */
public class SaslHandlersTest {
  private AlluxioConfiguration mConfiguration = ConfigurationTestUtils.defaults();

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void testCreateClientSimpleNullSubject() throws UnauthenticatedException {
    // Test null subject
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("client subject not provided");
    SaslClientHandler client = new SaslClientHandlerPlain(null, mConfiguration);
  }

  @Test
  public void testCreateClientSimpleEmptySubject() throws UnauthenticatedException {
    // Test subject with no user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClientHandler client = new SaslClientHandlerPlain(new Subject(), mConfiguration);
  }

  @Test
  public void testCreateClientSimpleNullUser() throws UnauthenticatedException {
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClientHandler client = new SaslClientHandlerPlain(null, null, null);
  }

  @Test
  public void testCreateClientSimpleNullPasword() throws UnauthenticatedException {
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClientHandler client = new SaslClientHandlerPlain("test", null, null);
  }
}
