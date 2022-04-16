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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.ZonedDateTime;
import java.util.Map;
import java.time.temporal.ChronoUnit;

/**
 * Parser for getting auth info from query parameters.
 * <p>
 * See: https://docs.aws.amazon
 * .com/AmazonS3/latest/API/sigv4-query-string-auth.html
 */
public class AuthorizationV4QueryParser implements SignatureParser {

  private static final Logger LOG =
            LoggerFactory.getLogger(AuthorizationV4QueryParser.class);

  private final Map<String, String> mQueryParameters;

  /**
   * Constructs a new {@link AuthorizationV4QueryParser}.
   * @param queryParameters
   */
  public AuthorizationV4QueryParser(Map<String, String> queryParameters) {
    mQueryParameters = queryParameters;
  }

  @Override
  public SignatureInfo parseSignature() throws S3Exception {
    if (!mQueryParameters.containsKey(SignerConstants.X_AMZ_SIGNATURE)) {
      return null;
    }
    validateDateAndExpires();
    final String rawCredential = mQueryParameters.get(SignerConstants.X_AMZ_CREDENTIAL);

    AwsCredential credential = null;
    try {
      credential = new AwsCredential(URLDecoder.decode(rawCredential, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(
                    "X-Amz-Credential is not proper URL encoded");
    }

    return new SignatureInfo(
                SignatureInfo.Version.V4,
                credential.getDate(),
                mQueryParameters.get(SignerConstants.X_AMZ_DATE),
                credential.getAccessKeyID(),
                mQueryParameters.get(SignerConstants.X_AMZ_SIGNATURE),
                mQueryParameters.get(SignerConstants.X_AMZ_SIGNED_HEADER),
                credential.createScope(),
                mQueryParameters.get(SignerConstants.X_AMZ_ALGORITHM),
                false
        );
  }

  protected void validateDateAndExpires() {
    final String dateString = mQueryParameters.get(SignerConstants.X_AMZ_DATE);
    final String expiresString = mQueryParameters.get(SignerConstants.X_AMZ_EXPIRES);
    if (expiresString != null && expiresString.length() > 0) {
      final Long expires = Long.valueOf(expiresString);

      if (ZonedDateTime.parse(dateString, SignerConstants.TIME_FORMATTER)
            .plus(expires, ChronoUnit.SECONDS).isBefore(ZonedDateTime.now())) {
        throw new IllegalArgumentException("Pre-signed S3 url is expired");
      }
    }
  }
}
