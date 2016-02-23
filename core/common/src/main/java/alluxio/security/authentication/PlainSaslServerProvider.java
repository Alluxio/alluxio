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

import java.security.Provider;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

/**
 * The Java SunSASL provider supports CRAM-MD5, DIGEST-MD5 and GSSAPI mechanisms on the server side.
 * When the SASL is using PLAIN mechanism, there is no support the SASL server. So there is a new
 * provider needed to register to support server-side PLAIN mechanism.
 */
@ThreadSafe
public final class PlainSaslServerProvider extends Provider {
  private static final long serialVersionUID = 4583558117355348638L;

  public static final String PROVIDER_NAME = "SaslPlain";
  public static final String MECHANISM = "PLAIN";
  public static final double PROVIDER_VERSION = 1.0;

  /**
   * Constructs a new provider for the SASL server when using the PLAIN mechanism.
   */
  public PlainSaslServerProvider() {
    super(PROVIDER_NAME, PROVIDER_VERSION, "Plain SASL server provider");
    put("SaslServerFactory." + MECHANISM, PlainSaslServerFactory.class.getName());
  }

  /**
   * This class is used to create an instances of {@link PlainSaslServer}. The parameter mechanism
   * must be "PLAIN" when this PlainSaslServerFactory is called, or null will be returned.
   */
  public static class PlainSaslServerFactory implements SaslServerFactory {
    /**
     * Creates a {@link SaslServer} using the parameters supplied. It returns null if no SaslServer
     * can be created using the parameters supplied. Throws {@link SaslException} if it cannot
     * create a SaslServer because of an error.
     *
     * @param mechanism the name of a SASL mechanism. (e.g. "PLAIN")
     * @param protocol the non-null string name of the protocol for which the authentication is
     *        being performed
     * @param serverName the non-null fully qualified host name of the server to authenticate to
     * @param props the possibly null set of properties used to select the SASL mechanism and to
     *        configure the authentication exchange of the selected mechanism
     * @param callbackHandler the possibly null callback handler to used by the SASL mechanisms to
     *        do further operation
     * @return A possibly null SaslServer created using the parameters supplied. If null, this
     *         factory cannot produce a SaslServer using the parameters supplied.
     * @exception SaslException If it cannot create a SaslServer because of an error.
     */
    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
        Map<String, ?> props, CallbackHandler callbackHandler) throws SaslException {
      if (MECHANISM.equals(mechanism)) {
        return new PlainSaslServer(callbackHandler);
      }
      return null;
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return new String[] {MECHANISM};
    }
  }
}
