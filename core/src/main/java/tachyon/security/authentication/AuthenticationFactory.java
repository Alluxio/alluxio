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

import javax.security.auth.login.LoginException;
import java.net.InetSocketAddress;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.MasterService;
import tachyon.util.NetworkUtils;

public class AuthenticationFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public enum AuthTypes {
    NOSASL("NOSASL"),
    SIMPLE("SIMPLE"),
    KERBEROS("KERBEROS");

    private final String mAuthType;

    AuthTypes(String mAuthType) {
      this.mAuthType = mAuthType;
    }

    public String getAuthName() {
      return mAuthType;
    }
  }

  private String mAuthTypeStr;
  private TachyonConf mTachyonConf;

  public AuthenticationFactory(TachyonConf mTachyonConf) {
    this.mTachyonConf = mTachyonConf;
    this.mAuthTypeStr = mTachyonConf.get(Constants.TACHYON_SECURITY_AUTHENTICATION,
        AuthTypes.SIMPLE.getAuthName());

    if (mAuthTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      // TODO: kerberos specific handling
    }
  }

  /**
   * Returns the thrift processor factory
   * @param service
   * @return
   */
  public TProcessorFactory getAuthProcFactory(MasterService.Iface service) {
    if (mAuthTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      // TODO: kerbores
      return null;
    } else {
      return PlainSaslHelper.getPlainProcessorFactory(service);
    }
  }

  public TTransportFactory getAuthTransFactory() {
    TTransportFactory tTransportFactory;

    if (mAuthTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      //TODO: kerbores
      throw new UnsupportedOperationException("Unsupported authentication type: " + mAuthTypeStr);
    } else if (mAuthTypeStr.equalsIgnoreCase(AuthTypes.NOSASL.getAuthName())) {
      tTransportFactory =  new TFramedTransport.Factory();
    } else if (mAuthTypeStr.equalsIgnoreCase(AuthTypes.SIMPLE.getAuthName())) {
      tTransportFactory = PlainSaslHelper.getPlainTransportFactory(mAuthTypeStr);
    } else {
      throw new UnsupportedOperationException("Unsupported authentication type: " + mAuthTypeStr);
    }
    return tTransportFactory;
  }

  public static TServerSocket createTServerSocket(InetSocketAddress address) throws
      TTransportException {
    return new TServerSocket(address);
  }

  public static TSocket createTSocket(InetSocketAddress address) {
    return new TSocket(NetworkUtils.getFqdnHost(address), address.getPort());
  }
}
