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
 * Fail-closed implementation of {@link Authenticator}. The method {@link #isAuthenticated} always
 * returns false, rejecting every request. This is the secure default so that the S3 proxy never
 * trusts an unverified client identity until a real signature-verifying {@link Authenticator} is
 * configured.
 */
public class DenyAllAuthenticator implements Authenticator {
  @Override
  public boolean isAuthenticated(AwsAuthInfo authInfo) throws S3Exception {
    return false;
  }
}
