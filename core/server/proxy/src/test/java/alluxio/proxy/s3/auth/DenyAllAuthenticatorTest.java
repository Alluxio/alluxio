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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import org.junit.Test;

/**
 * Regression test for the S3 proxy authentication-bypass: a forged AWS signature must not
 * authenticate under the shipped default configuration.
 */
public class DenyAllAuthenticatorTest {
  /** A completely fabricated signature, as used by the reported exploit. */
  private static final AwsAuthInfo FORGED_AUTH_INFO =
      new AwsAuthInfo("alluxio", "any-string-to-sign", "FAKESIGNATURE0000000000000000000000000000");

  @Test
  public void denyAllRejectsForgedSignature() throws Exception {
    assertFalse(new DenyAllAuthenticator().isAuthenticated(FORGED_AUTH_INFO));
  }

  @Test
  public void defaultConfigRejectsForgedSignature() throws Exception {
    AlluxioConfiguration conf = Configuration.global();
    // Signature verification must be on by default so the authenticator is consulted.
    assertTrue(conf.getBoolean(PropertyKey.S3_REST_AUTHENTICATION_ENABLED));
    // The default authenticator must be fail-closed, rejecting the forged signature.
    Authenticator authenticator = Authenticator.Factory.create(conf);
    assertFalse(authenticator.isAuthenticated(FORGED_AUTH_INFO));
  }
}
