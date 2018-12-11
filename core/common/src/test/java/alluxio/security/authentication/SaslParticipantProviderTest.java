package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.security.sasl.AuthenticationException;
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
  public void testCreateUnsupportedProvider() throws AuthenticationException {
    mThrown.expect(AuthenticationException.class);
    mThrown.expectMessage("Unsupported AuthType: " + AuthType.KERBEROS.getAuthName());
    SaslParticipiantProvider.Factory.create(AuthType.KERBEROS);

  }

  @Test
  public void testCreateSupportedProviders() throws AuthenticationException {
    SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    SaslParticipiantProvider.Factory.create(AuthType.CUSTOM);

  }

  @Test
  public void testCreateClientSimpleNullSubject()
      throws AuthenticationException, UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    Assert.assertNotNull(simpleProvider);
    // Test allow null subject
    SaslClient client = simpleProvider.getSaslClient(null);
    Assert.assertNotNull(client);
    Assert.assertEquals(PlainSaslServerProvider.MECHANISM, client.getMechanismName());
  }

  @Test
  public void testCreateClientSimpleNullUser()
      throws AuthenticationException, UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    Assert.assertNotNull(simpleProvider);
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClient client = simpleProvider.getSaslClient(null, null, null);
  }

  @Test
  public void testCreateClientSimpleNullPasword()
      throws AuthenticationException, UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    Assert.assertNotNull(simpleProvider);
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClient client = simpleProvider.getSaslClient("test", null, null);
  }

  @Test
  public void testCreateClientCustomNullUser()
          throws AuthenticationException, UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
            SaslParticipiantProvider.Factory.create(AuthType.CUSTOM);
    Assert.assertNotNull(simpleProvider);
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClient client = simpleProvider.getSaslClient(null, null, null);
  }

  @Test
  public void testCreateClientCustomNullPasword()
          throws AuthenticationException, UnauthenticatedException {
    SaslParticipiantProvider simpleProvider =
            SaslParticipiantProvider.Factory.create(AuthType.CUSTOM);
    Assert.assertNotNull(simpleProvider);
    // Test null user
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    SaslClient client = simpleProvider.getSaslClient("test", null, null);
  }

  @Test
  public void testCreateServerSimple() throws SaslException {
    SaslParticipiantProvider simpleProvider =
        SaslParticipiantProvider.Factory.create(AuthType.SIMPLE);
    Assert.assertNotNull(simpleProvider);
    SaslServer server = simpleProvider.getSaslServer("test");
    Assert.assertNotNull(server);
    Assert.assertEquals(PlainSaslServerProvider.MECHANISM, server.getMechanismName());
  }
}
