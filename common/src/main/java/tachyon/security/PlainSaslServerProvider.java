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

package tachyon.security;

import java.security.Provider;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

/**
 * Because the Java SunSASL provider doesn't support the server-side PLAIN mechanism.
 * There is a new provider needed to register to support server-side PLAIN mechanism.
 * There are three basic steps in implementing a SASL security provider:
 * 1.Write a class that implements the SaslServer interface {@link PlainSaslServer}
 * 2.Write a factory class implements the SaslServerFactory
 * 3.Write a JCA provider that registers the factory
 */
public class PlainSaslServerProvider extends Provider {
  public static final String TACHYON_PROVIDER_NAME = "TachyonSaslPlain";
  public static final String TACHYON_MECHANSIM_NAME = "PLAIN";

  public PlainSaslServerProvider() {
    super(TACHYON_PROVIDER_NAME, 1.0, "Tachyon Plain SASL provider");
    put("SaslServerFactory." + TACHYON_MECHANSIM_NAME, PlainSaslServerFactory.class.getName());
  }

  public static class PlainSaslServerFactory implements SaslServerFactory {
    /**
     * Creates a <tt>SaslServer</tt> using the parameters supplied.
     * It returns null if no <tt>SaslServer</tt> can be created using the parameters supplied.
     * Throws <tt>SaslException</tt> if it cannot create a <tt>SaslServer</tt>
     * because of an error.
     * @param mechanism The name of a SASL mechanism. (e.g. "PALIN").
     * @param protocol The non-null string name of the protocol for which
     * the authentication is being performed.
     * @param serverName The non-null fully qualified host name of the server
     * to authenticate to.
     * @param props The possibly null set of properties used to select the SASL
     * mechanism and to configure the authentication exchange of the selected
     * mechanism.
     *
     * @param cbh The possibly null callback handler to used by the SASL
     * mechanisms to do further operation.
     *
     *@return A possibly null <tt>SaslServer</tt> created using the parameters
     * supplied. If null, this factory cannot produce a <tt>SaslServer</tt>
     * using the parameters supplied.
     *@exception SaslException If cannot create a <tt>SaslServer</tt> because of an error.
     */
    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
                                       Map<String, ?> props, CallbackHandler cbh)
                                           throws SaslException {
      if (TACHYON_MECHANSIM_NAME.equals(mechanism)) {
        return new PlainSaslServer(cbh);
      }
      return null;
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return new String[] {TACHYON_MECHANSIM_NAME};
    }
  }
}
