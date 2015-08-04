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
import tachyon.security.PlainServerCallbackHandlerTest;
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
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    AuthenticationFactory authenticationFactory = new AuthenticationFactory(mTachyonConf);

    Assert.assertEquals(AuthenticationFactory.AuthType.SIMPLE, authenticationFactory.getAuthType());
  }

  @Test
  public void getAuthTypeFromConfTest() {
    AuthenticationFactory.AuthType authType;

    // should return a SIMPLE AuthType with conf "SIMPLE"
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");
    authType = AuthenticationFactory.getAuthTypeFromConf(mTachyonConf);
    Assert.assertEquals(AuthenticationFactory.AuthType.SIMPLE, authType);

    // should return a SIMPLE AuthType with conf "simple"
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "simple");
    authType = AuthenticationFactory.getAuthTypeFromConf(mTachyonConf);
    Assert.assertEquals(AuthenticationFactory.AuthType.SIMPLE, authType);

    // should throw exception with conf "wrong"
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "wrong");
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("wrong is not a valid authentication type. Check the configuration "
        + "parameter " + Constants.TACHYON_SECURITY_AUTHENTICATION);
    authType = AuthenticationFactory.getAuthTypeFromConf(mTachyonConf);
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
    TTransport client = createClient(null);
    client.open();
    Assert.assertTrue(client.isOpen());

    // clean up
    client.close();
    mServer.stop();
  }

  /**
   * In SIMPLE mode, the TTransport mechanism is PLAIN. When server authenticate the connected
   * client user, it use {@link tachyon.security.authentication.SimpleAuthenticationProviderImpl}.
   */
  @Test
  public void simpleAuthenticationTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    // start server
    startServerThread(mTachyonConf);

    // when connecting, authentication happens. It is a no-op in Simple mode.
    TTransport client = createClient("anyone");
    client.open();
    Assert.assertTrue(client.isOpen());

    // clean up
    client.close();
    mServer.stop();
  }

  /**
   * In CUSTOM mode, the TTransport mechanism is PLAIN. When server authenticate the connected
   * client user, it use configured AuthenticationProvider.
   */
  @Test
  public void customAuthenticationTest() throws Exception {
    mTachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "CUSTOM");
    mTachyonConf.set(Constants.TACHYON_AUTHENTICATION_PROVIDER_CUSTOM_CLASS,
        PlainServerCallbackHandlerTest.NameMatchAuthenticationProvider.class.getName());

    // start server
    startServerThread(mTachyonConf);

    // when connecting, authentication happens. Users starting with "tachyon" can pass.
    TTransport client = createClient("tachyon");
    client.open();
    Assert.assertTrue(client.isOpen());

    // Users not starting with "tachyon" can not pass, and throw exception.
    TTransport wrongClient = createClient("not-tachyon");
    mThrown.expect(TTransportException.class);
    mThrown.expectMessage("Peer indicated failure: AuthenticationProvider authenticate failed: "
        + "Only allow the user starting with tachyon");
    try {
      wrongClient.open();
    } catch (TTransportException e) {
      // clean up and re-throw the expected exception
      client.close();
      mServer.stop();
      throw e;
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

    mServer = new TThreadPoolServer(new TThreadPoolServer.Args(wrappedServerSocket)
        .maxWorkerThreads(2).minWorkerThreads(1)
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

    // ensure server is running
    while (!mServer.isServing() && serverThread.isAlive()) {
      Thread.sleep(50);
    }
  }

  // FIXME: API for creating client transport is on-going in TACHYON-621.
  // This code is temporarily used to simulate a client transport. Use the API when it's done.
  private TTransport createClient(String user) throws Exception {
    // the underlining client socket for connecting to server
    TTransport tTransport = new TSocket(NetworkAddressUtils.getFqdnHost(mServerAddress),
        mServerAddress.getPort());

    // wrap above socket.
    if (user != null) {
      // Simple and Custom mode
      return new TSaslClientTransport("PLAIN", null, null, null, new HashMap<String,
          String>(), new PlainCallbackHandler(user, "noPassword"), tTransport);
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
  private static class PlainCallbackHandler implements CallbackHandler {

    private final String mUserName;
    private final String mPassword;

    public PlainCallbackHandler(String mUserName, String mPassword) {
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
          passCallback.setPassword(mPassword.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }
    }
  }
}
