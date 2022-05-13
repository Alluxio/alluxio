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

import static alluxio.proxy.s3.S3Constants.S3_SIGN_CREDENTIAL;
import static alluxio.proxy.s3.S3Constants.S3_SIGN_SIGNATURE;
import static alluxio.proxy.s3.S3Constants.S3_SIGN_DATE;
import static alluxio.proxy.s3.S3Constants.S3_SIGN_SIGNED_HEADER;
import static alluxio.proxy.s3.S3Constants.S3_SIGN_ALGORITHM;
import static alluxio.proxy.s3.S3Constants.S3_SIGN_EXPIRES;
import static alluxio.proxy.s3.S3Constants.TIME_FORMATTER;
import static alluxio.proxy.s3.S3Constants.AUTHORIZATION_CHARSET;

import alluxio.proxy.s3.S3Exception;
import alluxio.proxy.s3.signature.AwsCredential;
import alluxio.proxy.s3.signature.SignatureInfo;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * Util for getting auth info from query parameters.
 * <p>
 * See: https://docs.aws.amazon
 * .com/AmazonS3/latest/API/sigv4-query-string-auth.html
 */
public final class AwsAuthV4QueryParserUtils {

  /**
   * Function to parse signature.
   * @param queryParameters
   * @return SignatureInfo instance
   * @throws S3Exception
   */
  public static SignatureInfo parseSignature(Map<String, String> queryParameters)
          throws S3Exception {
    if (!queryParameters.containsKey(S3_SIGN_SIGNATURE)) {
      return null;
    }
    validateDateAndExpires(queryParameters);
    final String rawCredential = queryParameters.get(S3_SIGN_CREDENTIAL);

    AwsCredential credential = null;
    try {
      credential = AwsCredential.create(URLDecoder.decode(rawCredential,
          AUTHORIZATION_CHARSET.name()));
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("X-Amz-Credential is not proper URL encoded");
    }

    return new SignatureInfo(
        SignatureInfo.Version.V4,
        credential.getDate(),
        queryParameters.get(S3_SIGN_DATE),
        credential.getAccessKeyID(),
        queryParameters.get(S3_SIGN_SIGNATURE),
        queryParameters.get(S3_SIGN_SIGNED_HEADER),
        credential.createScope(),
        queryParameters.get(S3_SIGN_ALGORITHM),
        false
    );
  }

  protected static void validateDateAndExpires(Map<String, String> queryParameters) {
    final String dateString = queryParameters.get(S3_SIGN_DATE);
    final String expiresString = queryParameters.get(S3_SIGN_EXPIRES);
    if (expiresString != null && expiresString.length() > 0) {
      final Long expires = Long.valueOf(expiresString);

      if (ZonedDateTime.parse(dateString, TIME_FORMATTER)
                  .plus(expires, ChronoUnit.SECONDS).isBefore(ZonedDateTime.now())) {
        throw new IllegalArgumentException("Pre-signed S3 url is expired");
      }
    }
  }
}
