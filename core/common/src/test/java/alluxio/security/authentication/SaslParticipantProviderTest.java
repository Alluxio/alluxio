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

import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.authentication.plain.PlainSaslServerProvider;
import alluxio.security.authentication.plain.SaslParticipiantProviderPlain;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Tests {@link SaslParticipiantProvider} and {@link SaslParticipiantProviderPlain}.
 */
public class SaslParticipantProviderTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void testCreateUnsupportedProvider() throws UnauthenticatedException {
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("Unsupported AuthType: " + AuthType.KERBEROS.getAuthName());
    SaslParticipiantProvider.Factory.create(AuthType.KERBEROS);
  }

  @Test
  public void testCreateSupportedProviders() throws UnauthenticatedException {
    SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    SaslParticipiantProvider.Factory.create(AuthType.CUSTOM);
  }

  @Test
  public void testCreateClientSimpleNullSubject() throws UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    Assert.assertNotNull(simpleProvider);
    // Test allow null subject
    SaslClient client = simpleProvider.createSaslClient(null);
    Assert.assertNotNull(client);
    Assert.assertEquals(PlainSaslServerProvider.MECHANISM, client.getMechanismName());
  }

  @Test
  public void testCreateClientSimpleNullUser() throws UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    Assert.assertNotNull(simpleProvider);
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClient client = simpleProvider.createSaslClient(null, null, null);
  }

  @Test
  public void testCreateClientSimpleNullPasword() throws UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    Assert.assertNotNull(simpleProvider);
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClient client = simpleProvider.createSaslClient("test", null, null);
  }

  @Test
  public void testCreateClientCustomNullUser() throws UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.CUSTOM);
    Assert.assertNotNull(simpleProvider);
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClient client = simpleProvider.createSaslClient(null, null, null);
  }

  @Test
  public void testCreateClientCustomNullPasword() throws UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.CUSTOM);
    Assert.assertNotNull(simpleProvider);
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClient client = simpleProvider.createSaslClient("test", null, null);
  }

  @Test
  public void testCreateServerSimple() throws UnauthenticatedException, SaslException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    Assert.assertNotNull(simpleProvider);
    SaslServer server = simpleProvider.createSaslServer("test");
    Assert.assertNotNull(server);
    Assert.assertEquals(PlainSaslServerProvider.MECHANISM, server.getMechanismName());
  }
}
