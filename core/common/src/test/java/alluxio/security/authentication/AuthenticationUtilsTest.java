/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetSocketAddress;

import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;

/**
 * Unit test for methods of {@link AuthenticationUtils}
 *
 * In order to test methods that return kinds of TTransport for connection in different mode, we
 * build Thrift servers and clients with specific TTransport, and let them connect.
 */
public class AuthenticationUtilsTest {

  private TThreadPoolServer mServer;
  private Configuration mConfiguration;
  private InetSocketAddress mServerAddress;
  private TServerSocket mServerTSocket;
  private TSocket mClientTSocket;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the server before running a test.
   *
   * @throws Exception thrown when the {@link TServerSocket} cannot be constructed
   */
  @Before
  public void before() throws Exception {
    mConfiguration = new Configuration();
    // Use port 0 to assign each test case an available port (possibly different)
    String localhost = NetworkAddressUtils.getLocalHostName(new Configuration());
    mServerTSocket = new TServerSocket(new InetSocketAddress(localhost, 0));
    int port = NetworkAddressUtils.getThriftPort(mServerTSocket);
    mServerAddress = new InetSocketAddress(localhost, port);
    mClientTSocket = AuthenticationUtils.createTSocket(mServerAddress,
        mConfiguration.getInt(Constants.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS));
  }

  /**
   * In NOSASL mode, the TTransport used should be the same as Alluxio original code.
   *
   * @throws Exception thrown when the server cannot be started
   */
  @Test
  public void nosaslAuthenticationTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "NOSASL");

    // start server
    startServerThread();

    // create client and connect to server
    TTransport client = AuthenticationUtils.getClientTransport(mConfiguration, mServerAddress);
    client.open();
    Assert.assertTrue(client.isOpen());

    // clean up
    client.close();
    mServer.stop();
  }

  /**
   * In SIMPLE mode, the TTransport mechanism is PLAIN. When server authenticate the connected
   * client user, it use {@link SimpleAuthenticationProvider}.
   *
   * @throws Exception thrown when the server cannot be started or the retrieval of the plain client
   *                   transport fails
   */
  @Test
  public void simpleAuthenticationTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");

    // start server
    startServerThread();

    // when connecting, authentication happens. It is a no-op in Simple mode.
    TTransport client =
        PlainSaslUtils.getPlainClientTransport("anyone", "whatever", mClientTSocket);
    client.open();
    Assert.assertTrue(client.isOpen());

    // clean up
    client.close();
    mServer.stop();
  }

  /**
   * In SIMPLE mode, if client's username is null, an exception should be thrown in client side.
   *
   * @throws Exception thrown when the retrieval of the plain client transport fails
   */
  @Test
  public void simpleAuthenticationNullUserTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");

    // check case that user is null
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    PlainSaslUtils.getPlainClientTransport(null, "whatever", mClientTSocket);
  }

  /**
   * In SIMPLE mode, if client's password is null, an exception should be thrown in client side.
   *
   * @throws Exception thrown when the retrieval of the plain client transport fails
   */
  @Test
  public void simpleAuthenticationNullPasswordTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");

    // check case that password is null
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    PlainSaslUtils.getPlainClientTransport("anyone", null, mClientTSocket);
  }

  /**
   * In SIMPLE mode, if client's username is empty, an exception should be thrown in server side.
   *
   * @throws Exception thrown when the server cannot be started or the retrieval of the plain client
   *                   transport fails
   */
  @Test
  public void simpleAuthenticationEmptyUserTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");

    // start server
    startServerThread();

    // check case that user is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No authentication"
        + " identity provided");
    TTransport client = PlainSaslUtils.getPlainClientTransport("", "whatever", mClientTSocket);
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
   *
   * @throws Exception thrown when the server cannot be started or the retrieval of the plain client
   *                   transport fails
   */
  @Test
  public void simpleAuthenticationEmptyPasswordTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");

    // start server
    startServerThread();

    // check case that password is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No password "
        + "provided");
    TTransport client = PlainSaslUtils.getPlainClientTransport("anyone", "", mClientTSocket);
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
   *
   * @throws Exception thrown when the server cannot be started or the retrieval of the plain client
   *                   transport fails
   */
  @Test
  public void customAuthenticationExactNamePasswordMatchTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM");
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        ExactlyMatchAuthenticationProvider.class.getName());

    // start server
    startServerThread();

    // when connecting, authentication happens. User's name:pwd pair matches and auth pass.
    TTransport client =
        PlainSaslUtils.getPlainClientTransport("alluxio", "correct-password", mClientTSocket);
    client.open();
    Assert.assertTrue(client.isOpen());

    // clean up
    client.close();
    mServer.stop();
  }

  /**
   * In CUSTOM mode, If the username:password pair does not match based on the configured
   * AuthenticationProvider, an exception should be thrown in server side.
   *
   * @throws Exception thrown when the server cannot be started or the retrieval of the plain client
   *                   transport fails
   */
  @Test
  public void customAuthenticationExactNamePasswordNotMatchTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM");
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        ExactlyMatchAuthenticationProvider.class.getName());

    // start server
    startServerThread();

    // User with wrong password can not pass auth, and throw exception.
    TTransport wrongClient =
        PlainSaslUtils.getPlainClientTransport("alluxio", "wrong-password", mClientTSocket);
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: "
        + "User authentication fails");
    try {
      wrongClient.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * In CUSTOM mode, if client's username is null, an exception should be thrown in client side.
   *
   * @throws Exception thrown when the retrieval of the plain client transport fails
   */
  @Test
  public void customAuthenticationNullUserTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM");

    // check case that user is null
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    PlainSaslUtils.getPlainClientTransport(null, "correct-password", mClientTSocket);
  }

  /**
   * In CUSTOM mode, if client's password is null, an exception should be thrown in client side.
   *
   * @throws Exception thrown when the retrieval of the plain client transport fails
   */
  @Test
  public void customAuthenticationNullPasswordTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM");

    // check case that password is null
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    PlainSaslUtils.getPlainClientTransport("alluxio", null, mClientTSocket);
  }

  /**
   * In CUSTOM mode, if client's username is empty, an exception should be thrown in server side.
   *
   * @throws Exception thrown when the server cannot be started or the retrieval of the plain client
   *                   transport fails
   */
  @Test
  public void customAuthenticationEmptyUserTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM");
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        ExactlyMatchAuthenticationProvider.class.getName());

    // start server
    startServerThread();

    // check case that user is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No authentication"
        + " identity provided");
    TTransport client =
        PlainSaslUtils.getPlainClientTransport("", "correct-password", mClientTSocket);
    try {
      client.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * In CUSTOM mode, if client's password is empty, an exception should be thrown in server side.
   *
   * @throws Exception thrown when the server cannot be started or the retrieval of the plain client
   *                   transport fails
   */
  @Test
  public void customAuthenticationEmptyPasswordTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM");
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        ExactlyMatchAuthenticationProvider.class.getName());

    // start server
    startServerThread();

    // check case that password is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No password "
        + "provided");
    TTransport client = PlainSaslUtils.getPlainClientTransport("alluxio", "", mClientTSocket);
    try {
      client.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * TODO(dong): In KERBEROS mode, ...
   * Tests that an exception is thrown when trying to use KERBEROS mode.
   *
   * @throws Exception thrown when the server cannot be started
   */
  @Test
  public void kerberosAuthenticationTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, "KERBEROS");

    // throw unsupported exception currently
    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("Kerberos is not supported currently.");
    startServerThread();
  }

  private void startServerThread() throws Exception {
    // create args and use them to build a Thrift TServer
    TTransportFactory tTransportFactory =
        AuthenticationUtils.getServerTransportFactory(mConfiguration);

    mServer =
        new TThreadPoolServer(new TThreadPoolServer.Args(mServerTSocket).maxWorkerThreads(2)
            .minWorkerThreads(1).processor(null).transportFactory(tTransportFactory)
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
   * This customized authentication provider is used in CUSTOM mode. It authenticate the user by
   * verifying the specific username:password pair.
   */
  public static class ExactlyMatchAuthenticationProvider implements AuthenticationProvider {
    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (!user.equals("alluxio") || !password.equals("correct-password")) {
        throw new AuthenticationException("User authentication fails");
      }
    }
  }

}
