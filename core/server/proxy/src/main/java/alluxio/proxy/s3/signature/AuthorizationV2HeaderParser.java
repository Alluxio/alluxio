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

import alluxio.proxy.s3.S3Exception;
import alluxio.proxy.s3.S3ErrorCode;

import org.apache.commons.lang3.StringUtils;

/**
 * Class to parse V2 auth information from header.
 */
public class AuthorizationV2HeaderParser implements SignatureParser {

  private static final String IDENTIFIER = "AWS ";

  private final String mAuthHeader;

  /**
   * @param authHeader authorization header
   */
  public AuthorizationV2HeaderParser(String authHeader) {
    mAuthHeader = authHeader;
  }

  /**
   * This method parses the authorization header.
   */
  @Override
  public SignatureInfo parseSignature() throws S3Exception {
    if (mAuthHeader == null || !mAuthHeader.startsWith(IDENTIFIER)) {
      return null;
    }
    String[] split = mAuthHeader.split(" ");
    if (split.length != 2) {
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    String[] remainingSplit = split[1].split(":");

    if (remainingSplit.length != 2) {
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    String accessKeyID = remainingSplit[0];
    String signature = remainingSplit[1];
    if (StringUtils.isBlank(accessKeyID) || StringUtils.isBlank(signature)) {
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
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
