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

package alluxio.proxy.s3.auth;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.proxy.s3.S3Exception;
import alluxio.util.CommonUtils;

/**
 * Interface to authenticate.
 */
public interface Authenticator {
  /**
   * Factory to create {@link Authenticator}.
   */
  class Factory {
    private Factory() {
    }

    /**
     * Creates and initializes {@link Authenticator} implementation.
     * based on Alluxio configuration.
     *
     * @return the generated {@link Authenticator} instance
     */
    public static Authenticator create(AlluxioConfiguration conf) {
      Authenticator authenticator = CommonUtils.createNewClassInstance(
                conf.getClass(PropertyKey.S3_REST_AUTHENTICATOR_CLASSNAME), null, null);
      return authenticator;
    }
  }

  /**
   * Check if the AwsAuthInfo is legal.
   * @param authInfo info for authentication
   * @return ture if this service should be accessed with authentication
   * @throws alluxio.proxy.s3.S3Exception
   */
  boolean isAuthenticated(AwsAuthInfo authInfo) throws S3Exception;
}
