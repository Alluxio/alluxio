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

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcConnection;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.security.User;
import alluxio.security.user.UserState;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;

import org.hamcrest.core.StringStartsWith;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

import javax.security.auth.Subject;
import javax.security.sasl.AuthenticationException;
import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Unit test for {@link alluxio.grpc.GrpcChannelBuilder} and {@link alluxio.grpc.GrpcServerBuilder}.
 */
public class GrpcSecurityTest {

  /** Timeout waiting for a closed stream. */
  private static final int S_AUTHENTICATION_PROPOGATE_TIMEOUT = 30000;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private InstancedConfiguration mConfiguration;

  @Before
  public void before() {
    mConfiguration = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  @Test
  public void testServerUnsupportedAuthentication() {
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(new StringStartsWith(false,
        "No factory could create a UserState with authType: " + AuthType.KERBEROS.name()));
    createServer(AuthType.KERBEROS);
  }

  @Test
  public void testSimpleAuthentication() throws Exception {
    GrpcServer server = createServer(AuthType.SIMPLE);
    try {
      server.start();
      UserState us = UserState.Factory.create(mConfiguration);
      GrpcChannelBuilder channelBuilder = GrpcChannelBuilder
          .newBuilder(getServerConnectAddress(server), mConfiguration).setSubject(us.getSubject());
      channelBuilder.build();
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testNoSaslAuthentication() throws Exception {
    GrpcServer server = createServer(AuthType.NOSASL);
    try {
      server.start();
      GrpcChannelBuilder channelBuilder =
          GrpcChannelBuilder.newBuilder(getServerConnectAddress(server), mConfiguration);
      channelBuilder.build();
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testCustomAuthentication() throws Exception {

    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());
    GrpcServer server = createServer(AuthType.CUSTOM);
    try {
      server.start();
      GrpcChannelBuilder channelBuilder =
          GrpcChannelBuilder.newBuilder(getServerConnectAddress(server), mConfiguration);
      channelBuilder.setSubject(createSubject(ExactlyMatchAuthenticationProvider.USERNAME,
          ExactlyMatchAuthenticationProvider.PASSWORD)).build();
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testCustomAuthenticationFails() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());
    GrpcServer server = createServer(AuthType.CUSTOM);
    try {
      server.start();
      GrpcChannelBuilder channelBuilder =
          GrpcChannelBuilder.newBuilder(getServerConnectAddress(server), mConfiguration);
      mThrown.expect(UnauthenticatedException.class);
      channelBuilder.setSubject(createSubject("fail", "fail")).build();
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testDisabledAuthentication() throws Exception {
    GrpcServer server = createServer(AuthType.SIMPLE);
    try {
      server.start();
      GrpcChannelBuilder channelBuilder =
          GrpcChannelBuilder.newBuilder(getServerConnectAddress(server), mConfiguration);
      channelBuilder.disableAuthentication().build();
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testAuthMismatch() throws Exception {
    GrpcServer server = createServer(AuthType.NOSASL);
    try {
      server.start();
      mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE);
      GrpcChannelBuilder channelBuilder =
          GrpcChannelBuilder.newBuilder(getServerConnectAddress(server), mConfiguration);
      mThrown.expect(UnauthenticatedException.class);
      channelBuilder.build();
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testAuthenticationClosed() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    GrpcServer server = createServer(AuthType.SIMPLE);
    try {
      server.start();
      UserState us = UserState.Factory.create(mConfiguration);
      GrpcChannel channel =
          GrpcChannelBuilder.newBuilder(getServerConnectAddress(server), mConfiguration)
              .setSubject(us.getSubject()).build();

      // Grab internal channel-Id.
      GrpcConnection connection = Whitebox.getInternalState(channel, "mConnection");
      UUID channelId = connection.getChannelKey().getChannelId();
      // Assert that authentication server has a login info for the channel.
      Assert.assertNotNull(server.getAuthenticationServer().getUserInfoForChannel(channelId));
      // Shutdown channel.
      channel.shutdown();
      // Assert that authentication server doesn't contain login info for the channel anymore.
      // Wait in a loop. Because closing the authentication will propagate asynchronously.
      CommonUtils.waitFor("login state removed", () -> {
        try {
          server.getAuthenticationServer().getUserInfoForChannel(channelId);
          return false;
        } catch (UnauthenticatedException exc) {
          return true;
        }
      }, WaitForOptions.defaults().setTimeoutMs(S_AUTHENTICATION_PROPOGATE_TIMEOUT));
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testAuthenticationRevoked() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mConfiguration.set(PropertyKey.AUTHENTICATION_INACTIVE_CHANNEL_REAUTHENTICATE_PERIOD, "250ms");
    GrpcServer server = createServer(AuthType.SIMPLE);
    try {
      server.start();
      UserState us = UserState.Factory.create(mConfiguration);
      GrpcChannel channel =
          GrpcChannelBuilder.newBuilder(getServerConnectAddress(server), mConfiguration)
              .setSubject(us.getSubject()).build();
      Assert.assertTrue(channel.isHealthy());
      /*
       * Sleeping will ensure that authentication sessions for the channel will expire on the
       * server. This should have propagated back to the client and its health status should reflect
       * that.
       *
       * Sleep more than authentication revocation timeout.
       */
      Thread.sleep(500);
      Assert.assertFalse(channel.isHealthy());
    } finally {
      server.shutdown();
    }
  }

  private GrpcServerAddress getServerConnectAddress(GrpcServer server) {
    return GrpcServerAddress.create(new InetSocketAddress(
        NetworkAddressUtils.getLocalHostName(
            (int) mConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
        server.getBindPort()));
  }

  private GrpcServer createServer(AuthType authType) {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, authType.name());
    InetSocketAddress bindAddress = new InetSocketAddress(NetworkAddressUtils.getLocalHostName(
        (int) mConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)), 0);
    UserState us = UserState.Factory.create(mConfiguration);
    GrpcServerBuilder serverBuilder = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create("localhost", bindAddress), mConfiguration, us);
    return serverBuilder.build();
  }

  private Subject createSubject(String username, String password) {
    Subject subject = new Subject();
    subject.getPrincipals().add(new User(username));
    subject.getPrivateCredentials().add(password);
    return subject;
  }

  /**
   * This customized authentication provider is used in CUSTOM mode. It authenticates the user by
   * verifying the specific username:password pair.
   */
  public static class ExactlyMatchAuthenticationProvider implements AuthenticationProvider {
    static final String USERNAME = "alluxio";
    static final String PASSWORD = "correct-password";

    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (!user.equals(USERNAME) || !password.equals(PASSWORD)) {
        throw new AuthenticationException("User authentication fails");
      }
    }
  }
}
