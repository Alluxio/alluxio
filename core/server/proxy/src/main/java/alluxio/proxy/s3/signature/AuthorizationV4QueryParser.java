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

import com.google.common.annotations.VisibleForTesting;
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
    if (!mQueryParameters.containsKey("X-Amz-Signature")) {
      return null;
    }
    validateDateAndExpires();

    final String rawCredential = mQueryParameters.get("X-Amz-Credential");

    Credential credential = null;
    try {
      credential = new Credential(URLDecoder.decode(rawCredential, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(
                    "X-Amz-Credential is not proper URL encoded");
    }

    return new SignatureInfo(
                SignatureInfo.Version.V4,
                credential.getDate(),
                mQueryParameters.get("X-Amz-Date"),
                credential.getAccessKeyID(),
                mQueryParameters.get("X-Amz-Signature"),
                mQueryParameters.get("X-Amz-SignedHeaders"),
                credential.createScope(),
                mQueryParameters.get("X-Amz-Algorithm"),
                false
        );
  }

  @VisibleForTesting
  protected void validateDateAndExpires() {
    final String dateString = mQueryParameters.get("X-Amz-Date");
    final String expiresString = mQueryParameters.get("X-Amz-Expires");
    if (expiresString != null && expiresString.length() > 0) {
      final Long expires = Long.valueOf(expiresString);

      if (ZonedDateTime.parse(dateString, StringToSignProducer.TIME_FORMATTER)
            .plus(expires, ChronoUnit.SECONDS).isBefore(ZonedDateTime.now())) {
        throw new IllegalArgumentException("Pre-signed S3 url is expired");
      }
    }
  }
}
