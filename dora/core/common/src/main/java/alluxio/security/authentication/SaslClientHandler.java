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

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Interface for authentication scheme specific {@link SaslClient} management.
 */
public interface SaslClientHandler extends AutoCloseable {
  /**
   * Handles the given {@link SaslMessage} from the server.
   *
   * @param message server-side Sasl message to handle
   * @return client's answer. null if client is completed
   * @throws SaslException
   */
  SaslMessage handleMessage(SaslMessage message) throws SaslException;

  /**
   * Close the handler and dispose internal resources.
   */
  @Override
  void close();
}
