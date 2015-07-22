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

public class PlainSaslServerProvider extends Provider {
  public static final String TACHYON_PLAIN_NAME = "TachyonSaslPlain";

  public PlainSaslServerProvider() {
    super(TACHYON_PLAIN_NAME, 1.0, "Tachyon Plain SASL provider");
    put("SaslServerFactory.PLAIN", SaslPlainServerFactory.class.getName());
  }

  public static class SaslPlainServerFactory implements SaslServerFactory {

    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
                                       Map<String, ?> props, CallbackHandler cbh) {
      if ("PLAIN".equals(mechanism)) {
        try {
          return new PlainSaslServer(cbh);
        } catch (SaslException e) {
          /* This is to fulfill the contract of the interface which states that an exception shall
             be thrown when a SaslServer cannot be created due to an error but null should be
             returned when a Server can't be created due to the parameters supplied. And the only
             thing PlainSaslServer can fail on is a non-supported authentication mechanism.
             That's why we return null instead of throwing the Exception */
          return null;
        }
      }
      return null;
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return new String[] {"PLAIN"};
    }
  }
}
