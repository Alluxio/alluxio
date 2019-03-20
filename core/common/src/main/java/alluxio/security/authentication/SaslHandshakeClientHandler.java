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

import javax.security.sasl.SaslException;

/**
 * Interface for providing client-side handshake routines for a particular authentication scheme.
 */
public interface SaslHandshakeClientHandler {
  /**
   * Handles the given {@link SaslMessage} from the server.
   *
   * @param message server-side Sasl message to handle
   * @return client's answer. null if client is completed
   * @throws SaslException
   */
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException;

  /**
   * @param channelId channe for which the authentication is happening
   * @return the initial message for Sasl traffic to begin
   * @throws SaslException
   */
  public SaslMessage getInitialMessage(String channelId) throws SaslException;
}
