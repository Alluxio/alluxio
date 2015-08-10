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

package tachyon.security.authentication;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.security.RemoteClientUser;
import tachyon.security.authentication.thrift.ClientUserTestService;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Unit test for inner class {@link tachyon.security.authentication.AuthenticationFactory
 * .AuthType} and methods of {@link tachyon.security.authentication.AuthenticationFactory}
 *
 * In order to test methods that return kinds of TTransport for connection in different mode,
 * we build Thrift servers and clients with specific TTransport, and let them connect.
 */
public class AuthenticationFactoryTest {

  TThreadPoolServer mServer;
  TachyonConf mTachyonConf = new TachyonConf();
  InetSocketAddress mServerAddress = new InetSocketAddress("localhost",
      Constants.DEFAULT_MASTER_PORT);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void authenticationFactoryConstructorTest() {
    AuthenticationFactory.AuthType authType;

    // should return a NOSASL AuthType with conf "NOSASL"
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "NOSASL");
    authType = new AuthenticationFactory(mTachyonConf).getAuthType();
    Assert.assertEquals(AuthenticationFactory.AuthType.NOSASL, authType);

    // should return a SIMPLE AuthType with conf "SIMPLE"
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");
    authType = new AuthenticationFactory(mTachyonConf).getAuthType();
    Assert.assertEquals(AuthenticationFactory.AuthType.SIMPLE, authType);

    // should return a CUSTOM AuthType with conf "CUSTOM"
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "CUSTOM");
    authType = new AuthenticationFactory(mTachyonConf).getAuthType();
    Assert.assertEquals(AuthenticationFactory.AuthType.CUSTOM, authType);

    // should return a KERBEROS AuthType with conf "KERBEROS"
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "KERBEROS");
    authType = new AuthenticationFactory(mTachyonConf).getAuthType();
    Assert.assertEquals(AuthenticationFactory.AuthType.KERBEROS, authType);

    // case insensitive - should return a SIMPLE AuthType with conf "simple"
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "simple");
    authType = new AuthenticationFactory(mTachyonConf).getAuthType();
    Assert.assertEquals(AuthenticationFactory.AuthType.SIMPLE, authType);

    // wrong configuration - should throw exception with conf "wrong"
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "wrong");
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("wrong is not a valid authentication type. Check the configuration "
        + "parameter " + Constants.TACHYON_SECURITY_AUTHENTICATION);
    authType = new AuthenticationFactory(mTachyonConf).getAuthType();
  }

  /**
   * In NOSASL mode, the TTransport used should be the same as Tachyon original code.
   */
  @Test
  public void nosaslAuthenticationTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "NOSASL");

    // start server
    startServerThread(mTachyonConf);

    // create client and connect to server
    TTransport client = createClient(mTachyonConf, null, null);
    client.open();
    Assert.assertTrue(client.isOpen());

    // clean up
    client.close();
    mServer.stop();
  }

  /**
   * In SIMPLE mode, the TTransport mechanism is PLAIN. When server authenticate the connected
   * client user, it use {@link tachyon.security.authentication.SimpleAuthenticationProviderImpl}.
   *
   * Two connections are built by two different users. The client user info should be maintained
   * in server threadlocal correctly.
   */
  @Test
  public void simpleAuthenticationTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    // start server
    startServerThread(mTachyonConf);

    // when connecting, authentication happens. It is a no-op in Simple mode.
    TTransport client = createClient(mTachyonConf, "anyone", "whatever");
    client.open();
    Assert.assertTrue(client.isOpen());

    // check user 'anyone'
    verifyClientUser(client, "anyone");

    // another connection by user 'anyone1'
    TTransport client1 = createClient(mTachyonConf, "anyone1", "whatever");
    client1.open();
    Assert.assertTrue(client1.isOpen());

    // check user 'anyone1'
    verifyClientUser(client1, "anyone1");

    // clean up
    client.close();
    client1.close();
    mServer.stop();
  }

  /**
   * In SIMPLE mode, if client's username is null, an exception should be thrown in client side.
   */
  @Test
  public void simpleAuthenticationNullUserTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    // check case that user is null
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    TTransport client = createClient(mTachyonConf, null, "whatever");
  }

  /**
   * In SIMPLE mode, if client's password is null, an exception should be thrown in client side.
   */
  @Test
  public void simpleAuthenticationNullPasswordTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    // check case that password is null
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    TTransport client = createClient(mTachyonConf, "anyone", null);
  }

  /**
   * In SIMPLE mode, if client's username is empty, an exception should be thrown in server side.
   */
  @Test
  public void simpleAuthenticationEmptyUserTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    // start server
    startServerThread(mTachyonConf);

    // check case that user is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No authentication"
        + " identity provided");
    TTransport client = createClient(mTachyonConf, "", "whatever");
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
  public void simpleAuthenticationEmptyPasswordTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    // start server
    startServerThread(mTachyonConf);

    // check case that password is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No password "
        + "provided");
    TTransport client = createClient(mTachyonConf, "anyone", "");
    try {
      client.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * In CUSTOM mode, the TTransport mechanism is PLAIN. When server authenticate the connected
   * client user, it use configured AuthenticationProvider.
   * If the username:password pair matches, a connection should be built.
   *
   * Two connections are built by two different users. The client user info should be maintained
   * in server threadlocal correctly.
   */
  @Test
  public void customAuthenticationExactNamePasswordMatchTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "CUSTOM");
    mTachyonConf.set(Constants.TACHYON_AUTHENTICATION_PROVIDER_CUSTOM_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());

    // start server
    startServerThread(mTachyonConf);

    // when connecting, authentication happens. User's name:pwd pair matches and auth pass.
    TTransport client = createClient(mTachyonConf, "tachyon", "correct-password");
    client.open();
    Assert.assertTrue(client.isOpen());

    // check user 'tachyon'
    verifyClientUser(client, "tachyon");

    // another connection by user 'tachyon1'
    TTransport client1 = createClient(mTachyonConf, "tachyon1", "correct-password1");
    client1.open();
    Assert.assertTrue(client1.isOpen());

    // check user 'tachyon1'
    verifyClientUser(client1, "tachyon1");

    // clean up
    client.close();
    client1.close();
    mServer.stop();
  }

  /**
   * In CUSTOM mode, If the username:password pair does not match based on the configured
   * AuthenticationProvider, an exception should be thrown in server side.
   */
  @Test
  public void customAuthenticationExactNamePasswordNotMatchTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "CUSTOM");
    mTachyonConf.set(Constants.TACHYON_AUTHENTICATION_PROVIDER_CUSTOM_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());

    // start server
    startServerThread(mTachyonConf);

    // User with wrong password can not pass auth, and throw exception.
    TTransport wrongClient = createClient(mTachyonConf, "tachyon", "wrong-password");
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
   */
  @Test
  public void customAuthenticationNullUserTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "CUSTOM");

    // check case that user is null
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    TTransport client = createClient(mTachyonConf, null, "correct-password");
  }

  /**
   * In CUSTOM mode, if client's password is null, an exception should be thrown in client side.
   */
  @Test
  public void customAuthenticationNullPasswordTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "CUSTOM");

    // check case that password is null
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("PLAIN: authorization ID and password must be specified");
    TTransport client = createClient(mTachyonConf, "tachyon", null);
  }

  /**
   * In CUSTOM mode, if client's username is empty, an exception should be thrown in server side.
   */
  @Test
  public void customAuthenticationEmptyUserTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "CUSTOM");
    mTachyonConf.set(Constants.TACHYON_AUTHENTICATION_PROVIDER_CUSTOM_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());

    // start server
    startServerThread(mTachyonConf);

    // check case that user is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No authentication"
        + " identity provided");
    TTransport client = createClient(mTachyonConf, "", "correct-password");
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
  public void customAuthenticationEmptyPasswordTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "CUSTOM");
    mTachyonConf.set(Constants.TACHYON_AUTHENTICATION_PROVIDER_CUSTOM_CLASS,
        ExactlyMatchAuthenticationProvider.class.getName());

    // start server
    startServerThread(mTachyonConf);

    // check case that password is empty
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: Plain authentication failed: No password "
        + "provided");
    TTransport client = createClient(mTachyonConf, "tachyon", "");
    try {
      client.open();
    } finally {
      mServer.stop();
    }
  }

  /**
   * TODO: In KERBEROS mode, ...
   */
  @Test
  public void kerberosAuthenticationTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "KERBEROS");

    // throw unsupported exception currently
    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("Kerberos is not supported currently.");
    startServerThread(mTachyonConf);
  }

  private void startServerThread(TachyonConf conf) throws Exception {
    // create args and use them to build a Thrift TServer
    AuthenticationFactory factory = new AuthenticationFactory(conf);
    TTransportFactory tTransportFactory = factory.getServerTransportFactory();

    TServerSocket wrappedServerSocket = new TServerSocket(mServerAddress);

    ClientUserTestServiceHandler handler = new ClientUserTestServiceHandler();
    ClientUserTestService.Processor<ClientUserTestServiceHandler> processor = new
        ClientUserTestService.Processor<ClientUserTestServiceHandler>(handler);

    mServer = new TThreadPoolServer(new TThreadPoolServer.Args(wrappedServerSocket)
        .maxWorkerThreads(2).minWorkerThreads(1)
        .processor(processor).transportFactory(tTransportFactory)
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
      count --;
    }
  }

  /**
   * Build a Thrift RPC client based on the transport, and then invoke RPC method to get the
   * client username maintained in server. Check whether it equals the connected one.
   * @param clientTransport
   * @param userName
   * @throws Exception
   */
  private void verifyClientUser(TTransport clientTransport, String userName) throws Exception {
    ClientUserTestService.Client mClient = new ClientUserTestService.Client(
        new TBinaryProtocol(clientTransport));
    Assert.assertEquals(userName, mClient.whoAmI());
  }

  /**
   * This customized authentication provider is used in CUSTOM mode. It authenticate the user by
   * verifying the specific username:password pair.
   */
  public static class ExactlyMatchAuthenticationProvider implements AuthenticationProvider {
    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (user.equals("tachyon")) {
        if (password.equals("correct-password")) {
          return;
        }
      } else if (user.equals("tachyon1")) {
        if (password.equals("correct-password1")) {
          return;
        }
      }
      throw new AuthenticationException("User authentication fails");
    }
  }

  /**
   * In order to test whether the client user is saved in the threadlocal variable of server
   * side, a very simple thrift RPC service is defined at {@link tachyon.security.authentication
   * .thrift.ClientUserTestService}. This class is the server side handler implementation of it.
   *
   * This handler fetch the client user maintained in threadlocal and return it to client for
   * verification. The returned user should be the same as the one used for building connection.
   */
  private static class ClientUserTestServiceHandler implements ClientUserTestService.Iface {
    @Override
    public String whoAmI() {
      return RemoteClientUser.get().getName();
    }
  }

  // FIXME: API for creating client transport is on-going in TACHYON-621.
  // This code is temporarily used to simulate a client transport. Use the API when it's done.
  private TTransport createClient(TachyonConf conf, String user, String password)
      throws SaslException {
    // the underlining client socket for connecting to server
    TTransport tTransport = new TSocket(NetworkAddressUtils.getFqdnHost(mServerAddress),
        mServerAddress.getPort());

    // wrap above socket.
    if (AuthenticationFactory.getAuthTypeFromConf(conf) != AuthenticationFactory.AuthType.NOSASL) {
      // Simple and Custom mode
      return new TSaslClientTransport("PLAIN", null, null, null, new HashMap<String,
          String>(), new PlainClientCallbackHandler(user, password), tTransport);
    } else {
      // NOSASL mode. The original Tachyon logic
      return new TFramedTransport(tTransport);
    }
  }

  // FIXME: This client side Callback Handler is on-going in TACHYON-621.
  // This code is temporarily used for test and should be deleted after TACHYON-621 merged.
  /**
   * A client side callback to put application provided username/pwd into SASL transport.
   */
  private static class PlainClientCallbackHandler implements CallbackHandler {

    private final String mUserName;
    private final String mPassword;

    public PlainClientCallbackHandler(String mUserName, String mPassword) {
      this.mUserName = mUserName;
      this.mPassword = mPassword;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nameCallback = (NameCallback) callback;
          nameCallback.setName(mUserName);
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback passCallback = (PasswordCallback) callback;
          passCallback.setPassword(mPassword == null ? null : mPassword.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }
    }
  }
}
