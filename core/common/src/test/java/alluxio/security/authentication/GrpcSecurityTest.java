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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.security.sasl.AuthenticationException;
import java.net.InetSocketAddress;

/**
 * Unit test for {@link alluxio.grpc.GrpcChannelBuilder} and {@link alluxio.grpc.GrpcServerBuilder}.
 */
public class GrpcSecurityTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void testServerUnsupportedAuthentication() {
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage("Authentication type not supported:" + AuthType.KERBEROS.name());
    createServer(AuthType.KERBEROS);
  }

  @Test
  public void testSimpleAuthentication() throws Exception {
    GrpcServer server = createServer(AuthType.SIMPLE);
    server.start();
    GrpcChannelBuilder channelBuilder =
        GrpcChannelBuilder.forAddress(getServerConnectAddress(server));
    channelBuilder.build();
    server.shutdown();
  }

  @Test
  public void testNoSaslAuthentication() throws Exception {
    GrpcServer server = createServer(AuthType.NOSASL);
    server.start();
    GrpcChannelBuilder channelBuilder =
        GrpcChannelBuilder.forAddress(getServerConnectAddress(server));
    channelBuilder.build();
    server.shutdown();
  }

  @Test
  public void testCustomAuthentication() throws Exception {

    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());
    GrpcServer server = createServer(AuthType.CUSTOM);
    server.start();
    GrpcChannelBuilder channelBuilder =
        GrpcChannelBuilder.forAddress(getServerConnectAddress(server));
    channelBuilder.setCredentials(ExactlyMatchAuthenticationProvider.USERNAME,
        ExactlyMatchAuthenticationProvider.PASSWORD, null).build();
    server.shutdown();
  }

  @Test
  public void testCustomAuthenticationFails() throws Exception {

    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());
    GrpcServer server = createServer(AuthType.CUSTOM);
    server.start();
    GrpcChannelBuilder channelBuilder =
        GrpcChannelBuilder.forAddress(getServerConnectAddress(server));
    mThrown.expect(UnauthenticatedException.class);
    channelBuilder.setCredentials("fail", "fail", null).build();
    server.shutdown();
  }

  @Test
  public void testDisabledAuthentication() throws Exception {
    GrpcServer server = createServer(AuthType.SIMPLE);
    server.start();
    GrpcChannelBuilder channelBuilder =
        GrpcChannelBuilder.forAddress(getServerConnectAddress(server));
    channelBuilder.disableAuthentication().build();
    server.shutdown();
  }

  @Test
  public void testAuthMismatch() throws Exception {
    GrpcServer server = createServer(AuthType.NOSASL);
    server.start();
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE);
    GrpcChannelBuilder channelBuilder =
        GrpcChannelBuilder.forAddress(getServerConnectAddress(server));
    mThrown.expect(UnauthenticatedException.class);
    channelBuilder.build();
    server.shutdown();
  }

  private InetSocketAddress getServerConnectAddress(GrpcServer server) {
    return new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), server.getBindPort());
  }

  private GrpcServer createServer(AuthType authType) {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, authType.name());
    GrpcServerBuilder serverBuilder = GrpcServerBuilder
        .forAddress(new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), 0));
    return serverBuilder.build();
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
