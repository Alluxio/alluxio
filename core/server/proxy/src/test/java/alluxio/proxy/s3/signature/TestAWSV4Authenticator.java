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

package alluxio.proxy.s3.signature;

import static org.junit.Assert.assertTrue;

import alluxio.proxy.s3.S3Exception;
import alluxio.proxy.s3.auth.Authenticator;
import alluxio.proxy.s3.auth.AwsAuthInfo;

import org.junit.Test;

public class TestAWSV4Authenticator {

  public static class DummyAWSAuthenticator implements Authenticator {

    @Override
    public boolean isAuthenticated(AwsAuthInfo authInfo) throws S3Exception {
      return AuthorizationV4Validator.validateRequest(
              authInfo.getStringTosSign(),
              authInfo.getSignature(),
              getSecret()
        );
    }

    private String getSecret() {
      return "testpassword";
    }
  }

  @Test
  public void testAuthenticator() throws Exception {
    String stringToSign = "AWS4-HMAC-SHA256\n"
            + "20220316T083426Z\n"
            + "20220316/us-east-1/s3/aws4_request\n"
            + "3b88db84484342519cb20c2722553a0f1b15402dbe69acaad78da8f825ace20f";
    String signature = "e21cc9301f70ff9ffe7ff0e940221da9bf1b7a2d4a586696aed3c7437254eb9f";
    String accessKeyId = "testuser";

    AwsAuthInfo authInfo = new AwsAuthInfo(accessKeyId, stringToSign, signature);
    DummyAWSAuthenticator authenticator = new DummyAWSAuthenticator();
    assertTrue(authenticator.isAuthenticated(authInfo));
  }
}
