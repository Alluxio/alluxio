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
import java.security.Security;
import java.util.HashMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import tachyon.thrift.MasterService;

public class PlainSaslHelper {

  // Register Plain SASL server provider
  static {
    Security.addProvider(new SaslPlainServerProvider());
  }

  public static TProcessorFactory getPlainProcessorFactory(MasterService.Iface service) {
    return new PlainProcessorFactory(service);
  }

  private static final class PlainProcessorFactory extends TProcessorFactory {

    private final MasterService.Iface mService;

    PlainProcessorFactory(MasterService.Iface mService) {
      super(null);
      this.mService = mService;
    }

    @Override
    public TProcessor getProcessor(TTransport trans) {
      return new TSetUserProcessor<MasterService.Iface>(mService);
    }
  }

  public static TTransportFactory getPlainTransportFactory(String authType) {
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    saslFactory.addServerDefinition("PLAIN", authType, null, new HashMap<String, String>(),
        new PlainServerCallbackHandler(authType));

    return saslFactory;
  }

  private static final class PlainServerCallbackHandler implements CallbackHandler {

    private final AuthenticationProviderFactory.AuthenticationMethod mAuthMethod;

    PlainServerCallbackHandler(String authMethodStr) {
      mAuthMethod = AuthenticationProviderFactory.AuthenticationMethod
          .getValidAuthenticationMethod(authMethodStr);
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      String username = null;
      String password = null;
      AuthorizeCallback ac = null;

      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nc = (NameCallback) callback;
          username = nc.getName();
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback pc = (PasswordCallback) callback;
          password = new String(pc.getPassword());
        } else if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }

      AuthenticationProviderFactory.AuthenticationProvider provider =
          AuthenticationProviderFactory.getAuthenticationProvider(mAuthMethod);
      provider.authenticate(username, password);
      if (ac != null) {
        ac.setAuthorized(true);
      }
    }
  }

  public static TTransport getPlainTransport(String username, String password,
                                             TTransport wrappedTransport) throws SaslException {
    return new TSaslClientTransport("PLAIN", null, null, null, new HashMap<String,
        String>(), new PlainCallbackHandler(username, password), wrappedTransport);
  }

  public static class PlainCallbackHandler implements CallbackHandler {

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
