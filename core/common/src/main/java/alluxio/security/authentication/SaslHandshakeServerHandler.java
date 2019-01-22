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

import alluxio.grpc.SaslMessage;
import alluxio.security.authentication.plain.SaslHandshakeServerHandlerPlain;

import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Interface for providing server-side handshake routines for a particular authentication scheme.
 */
public interface SaslHandshakeServerHandler {
  /**
   * Handles given {@link SaslMessage} from the client.
   *
   * @param message client Sasl message
   * @return server's response to given client message
   * @throws SaslException
   */
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException;

  /**
   * Factory for {@link SaslHandshakeServerHandler}.
   */
  class Factory {

    // prevent instantiation
    private Factory() {}

    /**
     * @param authType authentication type to use
     * @return the generated {@link AuthenticationProvider}
     * @throws AuthenticationException when unsupported authentication type is used
     */
    public static SaslHandshakeServerHandler create(AuthType authType, SaslServer saslServer)
        throws AuthenticationException {
      switch (authType) {
        case SIMPLE:
        case CUSTOM:
          return new SaslHandshakeServerHandlerPlain(saslServer);
        default:
          throw new AuthenticationException("Unsupported AuthType: " + authType.getAuthName());
      }
    }
  }
}
