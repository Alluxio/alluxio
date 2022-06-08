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

import static alluxio.proxy.s3.S3Constants.DATE_FORMATTER;
import static org.junit.Assert.assertEquals;

import alluxio.proxy.s3.S3Exception;
import alluxio.proxy.s3.signature.utils.AwsAuthV4HeaderParserUtils;

import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

/**
 * This class tests Authorization header format v4.
 */
public class TestAuthorizationV4HeaderParser {

  private String mCurDate;

  @Before
  public void setup() {
    LocalDate now = LocalDate.now();
    mCurDate = DATE_FORMATTER.format(now);
  }

  @Test
  public void testAuthHeaderV4() throws Exception {
    String auth = "AWS4-HMAC-SHA256 "
            + "Credential=testuser/"
            + mCurDate
            + "/us-east-1/s3/aws4_request, "
            + "SignedHeaders=host;range;x-amz-date, "
            + "Signature=e21cc9301f70ff9ffe7ff0e940221daa";
    String date = "20220316T083426Z";
    final SignatureInfo signatureInfo = AwsAuthV4HeaderParserUtils.parseSignature(auth, date);
    assertEquals("testuser", signatureInfo.getAwsAccessId());
    assertEquals(mCurDate, signatureInfo.getDate());
    assertEquals("host;range;x-amz-date", signatureInfo.getSignedHeaders());
    assertEquals("e21cc9301f70ff9ffe7ff0e940221daa",
                signatureInfo.getSignature());
  }

  @Test
  public void testIncorrectHeader() throws S3Exception {
    try {
      String auth = "AWS4-HMAC-SHA256 "
              + "Credential=testuser/20220316/us-east-1/s3/aws4_request, "
              + "Signature=e21cc9301f70ff9ffe7ff0e940221da";
      String date = "20220316T083426Z";
      AwsAuthV4HeaderParserUtils.parseSignature(auth, date);
    } catch (S3Exception e) {
      assertEquals("AuthorizationHeaderMalformed", e.getErrorCode().getCode());
    }
  }
}
