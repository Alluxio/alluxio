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
import static org.junit.Assert.fail;

import alluxio.proxy.s3.S3Exception;
import alluxio.proxy.s3.signature.utils.AwsAuthV2HeaderParserUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests Authorization header format v2.
 */
public class TestAuthorizationV2HeaderParser {
  @Test
  public void testAuthHeaderV2() throws S3Exception {
    try {
      String auth = "AWS accessKey:signature";
      final SignatureInfo signatureInfo = AwsAuthV2HeaderParserUtils.parseSignature(auth);
      assertEquals(signatureInfo.getAwsAccessId(), "accessKey");
      assertEquals(signatureInfo.getSignature(), "signature");
    } catch (S3Exception ex) {
      fail("testAuthHeaderV2 failed");
    }
  }

  @Test
  public void testIncorrectHeader() throws S3Exception {
    String auth = "ASW accessKey:signature";
    Assert.assertNull(AwsAuthV2HeaderParserUtils.parseSignature(auth));
  }
}
