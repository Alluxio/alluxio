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

import static org.junit.Assert.assertEquals;

import alluxio.proxy.s3.S3Exception;

import org.junit.Test;

/**
 * This class tests Authorization header format v4.
 */
public class TestAuthorizationV4HeaderParser {
  @Test
  public void testAuthHeaderV4() throws Exception {
    String auth = "AWS4-HMAC-SHA256 "
            + "Credential=testuser/20220316/us-east-1/s3/aws4_request, "
            + "SignedHeaders=host;range;x-amz-date, "
            + "Signature=e21cc9301f70ff9ffe7ff0e940221da";
    AuthorizationV4HeaderParser v4 =
                new AuthorizationV4HeaderParser(auth, "20220316T083426Z");
    final SignatureInfo signatureInfo = v4.parseSignature();
    assertEquals("testuser", signatureInfo.getAwsAccessId());
    assertEquals("20220316", signatureInfo.getDate());
    assertEquals("host;range;x-amz-date", signatureInfo.getSignedHeaders());
    assertEquals("e21cc9301f70ff9ffe7ff0e940221da",
                signatureInfo.getSignature());
  }

  @Test
  public void testIncorrectHeader() throws S3Exception {
    try {
      String auth = "AWS4-HMAC-SHA256 "
              + "Credential=testuser/20220316/us-east-1/s3/aws4_request, "
              + "Signature=e21cc9301f70ff9ffe7ff0e940221da";
      AuthorizationV4HeaderParser v4 =
              new AuthorizationV4HeaderParser(auth, "20220316T083426Z");
      v4.parseSignature();
    } catch (S3Exception e) {
      assertEquals("AuthorizationHeaderMalformed", e.getErrorCode().getCode());
    }
  }
}
