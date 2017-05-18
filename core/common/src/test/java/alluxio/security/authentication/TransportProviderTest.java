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
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetSocketAddress;

import javax.security.sasl.AuthenticationException;

/**
 * Unit test for methods of {@link TransportProvider}.
 *
 * In order to test methods that return kinds of TTransport for connection in different mode, we
 * build Thrift servers and clients with specific TTransport, and let them connect.
 */
public final class TransportProviderTest {

  private TThreadPoolServer mServer;
  private InetSocketAddress mServerAddress;
  private TServerSocket mServerTSocket;
  private TransportProvider mTransportProvider;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the server before running a test.
   */
  @Before
  public void before() throws Exception {
    // Use port 0 to assign each test case an available port (possibly different)
    String localhost = NetworkAddressUtils.getLocalHostName();
    mServerTSocket = new TServerSocket(new InetSocketAddress(localhost, 0));
    int port = NetworkAddressUtils.getThriftPort(mServerTSocket);
    mServerAddress = new InetSocketAddress(localhost, port);
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * In NOSASL mode, the TTransport used should be the same as Alluxio original code.
   */
  @Test
  public void nosaslAuthentrication() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    // start server
    startServerThread();

    // create client and connect to server
    TTransport client = mTransportProvider.getClientTransport(mServerAddress);
    client.open();
    Assert.assertTrue(client.isOpen());

    // clean up
    client.close();
    mServer.stop();
  }

  /**
   * In SIMPLE mode, the TTransport mechanism is PLAIN. When server authenticate the connected
   * client user, it use {@link SimpleAuthenticationProvider}.
   */
  @Test
  public void simpleAuthentication() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    // start server
    startServerThread();

    // when connecting, authentication happens. It is a no-op in Simple mode.
    TTransport client = mTransportProvider.getClientTransport(mServerAddress);
    client.open();
    Assert.assertTrue(client.isOpen());

    // clean up
    client.close();
    mServer.stop();
  }

  /**
   * In SIMPLE mode, if client's username is null, an exception should be thrown in client side.
   */
  @Test
  public void simpleAuthenticationNullUser() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    // check case that user is null
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport(null, "whatever", mServerAddress);
  }

  /**
   * In SIMPLE mode, if client's password is null, an exception should be thrown in client side.
   */
  @Test
  public void simpleAuthenticationNullPassword() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    // check case that password is null
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport("anyone", null, mServerAddress);
  }

  /**
   * In SIMPLE mode, if client's username is empty, an exception should be thrown in server side.
   */
  @Test
  public void simpleAuthenticationEmptyUser() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    // start server
    startServerThread();

    // check case that user is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No authentication"
        + " identity provided");
    TTransport client = ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport("", "whatever", mServerAddress);
    try {
      client.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * In SIMPLE mode, if client's password is empty, an exception should be thrown in server side.
   * Although password is actually not used and we do not really authenticate the user in SIMPLE
   * mode, we need the Plain SASL server has ability to check empty password.
   */
  @Test
  public void simpleAuthenticationEmptyPassword() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    // start server
    startServerThread();

    // check case that password is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage(
        "Peer indicated failure: Plain authentication failed: No password " + "provided");
    TTransport client = ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport("anyone", "", mServerAddress);
    try {
      client.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * In CUSTOM mode, the TTransport mechanism is PLAIN. When server authenticate the connected
   * client user, it use configured AuthenticationProvider. If the username:password pair matches, a
   * connection should be built.
   */
  @Test
  public void customAuthenticationExactNamePasswordMatch() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());
    mTransportProvider = TransportProvider.Factory.create();

    // start server
    startServerThread();

    // when connecting, authentication happens. User's name:pwd pair matches and auth pass.
    TTransport client = ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport(ExactlyMatchAuthenticationProvider.USERNAME,
            ExactlyMatchAuthenticationProvider.PASSWORD, mServerAddress);
    client.open();
    Assert.assertTrue(client.isOpen());

    // clean up
    client.close();
    mServer.stop();
  }

  /**
   * In CUSTOM mode, If the username:password pair does not match based on the configured
   * AuthenticationProvider, an exception should be thrown in server side.
   */
  @Test
  public void customAuthenticationExactNamePasswordNotMatch() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());
    mTransportProvider = TransportProvider.Factory.create();

    // start server
    startServerThread();

    // User with wrong password can not pass auth, and throw exception.
    TTransport wrongClient = ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport(ExactlyMatchAuthenticationProvider.USERNAME, "wrong-password",
            mServerAddress);
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage(
        "Peer indicated failure: Plain authentication failed: " + "User authentication fails");
    try {
      wrongClient.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * In CUSTOM mode, if client's username is null, an exception should be thrown in client side.
   */
  @Test
  public void customAuthenticationNullUser() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    // check case that user is null
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport(null, ExactlyMatchAuthenticationProvider.PASSWORD, mServerAddress);
  }

  /**
   * In CUSTOM mode, if client's password is null, an exception should be thrown in client side.
   */
  @Test
  public void customAuthenticationNullPassword() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    // check case that password is null
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport(ExactlyMatchAuthenticationProvider.USERNAME, null, mServerAddress);
  }

  /**
   * In CUSTOM mode, if client's username is empty, an exception should be thrown in server side.
   */
  @Test
  public void customAuthenticationEmptyUser() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());
    mTransportProvider = TransportProvider.Factory.create();

    // start server
    startServerThread();

    // check case that user is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No authentication"
        + " identity provided");
    TTransport client = ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport("", ExactlyMatchAuthenticationProvider.PASSWORD, mServerAddress);
    try {
      client.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * In CUSTOM mode, if client's password is empty, an exception should be thrown in server side.
   */
  @Test
  public void customAuthenticationEmptyPassword() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());
    mTransportProvider = TransportProvider.Factory.create();

    // start server
    startServerThread();

    // check case that password is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage(
        "Peer indicated failure: Plain authentication failed: No password provided");
    TTransport client = ((PlainSaslTransportProvider) mTransportProvider)
        .getClientTransport(ExactlyMatchAuthenticationProvider.USERNAME, "", mServerAddress);
    try {
      client.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * TODO(dong): In KERBEROS mode, ...
   * Tests that an exception is thrown when trying to use KERBEROS mode.
   */
  @Test
  public void kerberosAuthentication() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());

    // throw unsupported exception currently
    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("Kerberos is not supported currently.");
    mTransportProvider = TransportProvider.Factory.create();
  }

  private void startServerThread() throws Exception {
    // create args and use them to build a Thrift TServer
    TTransportFactory tTransportFactory = mTransportProvider.getServerTransportFactory("test");

    mServer = new TThreadPoolServer(
        new TThreadPoolServer.Args(mServerTSocket).maxWorkerThreads(2).minWorkerThreads(1)
            .processor(null).transportFactory(tTransportFactory)
            .protocolFactory(new TBinaryProtocol.Factory(true, true)));

    // start the server in a new thread
    Thread serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        mServer.serve();
      }
    });

    serverThread.start();

    // ensure server is running, and break if it does not start serving in 2 seconds.
    int count = 40;
    while (!mServer.isServing() && serverThread.isAlive()) {
      if (count <= 0) {
        throw new RuntimeException("TThreadPoolServer does not start serving");
      }
      Thread.sleep(50);
      count--;
    }
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
