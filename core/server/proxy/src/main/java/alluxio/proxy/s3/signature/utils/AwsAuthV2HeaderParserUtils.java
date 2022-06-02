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

package alluxio.proxy.s3.signature.utils;

import alluxio.proxy.s3.S3ErrorCode;
import alluxio.proxy.s3.S3Exception;
import alluxio.proxy.s3.signature.SignatureInfo;

import org.apache.commons.lang3.StringUtils;

/**
 * Util to parse V2 auth information from header.
 */
public final class AwsAuthV2HeaderParserUtils {
  private static final String IDENTIFIER = "AWS ";

  /**
   * Function to parse signature.
   * @param authHeader Authorization header string
   * @return SignatureInfo instance
   * @throws S3Exception
   */
  public static SignatureInfo parseSignature(String authHeader) throws S3Exception {
    if (authHeader == null || !authHeader.startsWith(IDENTIFIER)) {
      return null;
    }
    String[] split = authHeader.split(" ");
    if (split.length != 2) {
      throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    String[] remainingSplit = split[1].split(":");

    if (remainingSplit.length != 2) {
      throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    String accessKeyID = remainingSplit[0];
    String signature = remainingSplit[1];
    if (StringUtils.isBlank(accessKeyID) || StringUtils.isBlank(signature)) {
      throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    return new SignatureInfo(
        SignatureInfo.Version.V2,
        "",
        "",
        accessKeyID,
        signature,
        "",
        "",
        "",
        false
    );
  }
}
