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

import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticationProvider;
import alluxio.util.CommonUtils;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.sasl.AuthenticationException;

/**
 * An authentication provider implementation that allows {@link AuthenticationProvider} to be
 * customized at configuration time. This authentication provider is created if authentication type
 * specified in {@link alluxio.conf.AlluxioConfiguration} is {@link AuthType#CUSTOM}. It requires
 * the property {@code alluxio.security.authentication.custom.provider} to be set in
 * {@link alluxio.conf.AlluxioConfiguration} to determine which provider to load.
 */
@NotThreadSafe
public final class CustomAuthenticationProvider implements AuthenticationProvider {

  private final AuthenticationProvider mCustomProvider;

  /**
   * Constructs a new custom authentication provider.
   *
   * @param providerName the name of the provider
   */
  public CustomAuthenticationProvider(String providerName) {
    Class<?> customProviderClass;
    try {
      customProviderClass = Class.forName(providerName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(providerName + " not found");
    }

    mCustomProvider = (AuthenticationProvider) CommonUtils
        .createNewClassInstance(customProviderClass, null, null);
  }

  /**
   * @return the custom authentication provider
   */
  public AuthenticationProvider getCustomProvider() {
    return mCustomProvider;
  }

  @Override
  public void authenticate(String user, String password) throws AuthenticationException {
    mCustomProvider.authenticate(user, password);
  }
}
