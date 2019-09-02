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

import alluxio.grpc.ChannelAuthenticationScheme;

import javax.security.sasl.SaslClient;
import java.io.IOException;

/**
 * Interface for authentication scheme specific {@link SaslClient} management.
 */
public interface SaslClientHandler extends AutoCloseable {
  /**
   * @return the scheme under which client is authenticating
   */
  ChannelAuthenticationScheme getClientScheme();

  /**
   * @return the {@link SaslClient} instance
   */
  SaslClient getSaslClient();

  /**
   * Close the handler and dispose internal resources.
   */
  @Override
  void close() throws IOException;
}
