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

package alluxio.security.authentication.plain;

import alluxio.security.authentication.AuthenticationProvider;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.sasl.AuthenticationException;

/**
 * An authentication provider implementation that allows arbitrary combination of username and
 * password including empty strings.
 */
@ThreadSafe
public final class SimpleAuthenticationProvider implements AuthenticationProvider {

  /**
   * Constructs a new {@link SimpleAuthenticationProvider}.
   */
  public SimpleAuthenticationProvider() {}

  @Override
  public void authenticate(String user, String password) throws AuthenticationException {
    // no-op authentication
  }
}
