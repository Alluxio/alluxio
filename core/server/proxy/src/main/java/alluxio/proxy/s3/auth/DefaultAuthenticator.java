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

import alluxio.proxy.s3.S3Exception;

/**
 * Default implementation of {@link Authenticator}. The method {@link #isValid}
 * returns true by default.
 *
 * When defining how to obtain secret, the following method can be called for
 * authentication:
 *    return AuthorizationV4Validator.validateRequest(
 *                 signedInfo.getStringTosSign(),
 *                 signedInfo.getSignature(),
 *                 secret);
 *
 * Todo:
 *   1) define how to get secret
 *   2) use 'AuthorizationV4Validator.validateRequest' to check signature
 *
 */
public class DefaultAuthenticator implements Authenticator {
  @Override
  public boolean isValid(AwsAuthInfo authInfo) throws S3Exception {
    return true;
  }
}
