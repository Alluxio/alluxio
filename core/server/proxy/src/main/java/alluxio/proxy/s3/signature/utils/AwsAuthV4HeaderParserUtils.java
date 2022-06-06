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
import alluxio.proxy.s3.signature.AwsCredential;
import alluxio.proxy.s3.signature.SignatureInfo;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringTokenizer;

/**
 * Util to parse v4 auth information from header.
 */
public final class AwsAuthV4HeaderParserUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AwsAuthV4HeaderParserUtils.class);

  private static final String CREDENTIAL = "Credential=";
  private static final String SIGNEDHEADERS = "SignedHeaders=";
  private static final String SIGNATURE = "Signature=";
  private static final String AWS4_SIGNING_ALGORITHM = "AWS4-HMAC-SHA256";

  /**
   * Validate Signed headers.
   *
   * @param signedHeadersStr signed header
   * @return a string which parsed singed headers
   */
  private static String parseSignedHeaders(String authHeader, String signedHeadersStr)
          throws S3Exception {
    if (StringUtils.isNotEmpty(signedHeadersStr)
                && signedHeadersStr.startsWith(SIGNEDHEADERS)) {
      String parsedSignedHeaders = signedHeadersStr.substring(SIGNEDHEADERS.length());
      StringTokenizer tokenizer = new StringTokenizer(parsedSignedHeaders, ";");
      if (tokenizer.hasMoreTokens()) {
        return parsedSignedHeaders;
      } else {
        LOG.error("No signed headers found. AuthHeader:{}", authHeader);
        throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
    } else {
      LOG.error("No signed headers found. AuthHeader:{}", authHeader);
      throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }

  /**
   * Function to parse signature.
   *
   * Sample authHeader value:
   * AWS4-HMAC-SHA256 Credential=testuser/20220510/us-east-1/s3/aws4_request, \
   * SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;host;x-amz-content-sha256;x-amz-date, \
   * Signature=687370ebf9278c5c0b1ef744aa84026d471e074701fd31817012dc54d5c2cedc
   *
   * Sample dateHeader value:
   * 20220510T081815Z
   *
   * @param authHeader authorization header string
   * @param dateHeader date string
   * @return SignatureInfo instance
   * @throws S3Exception
   */
  public static SignatureInfo parseSignature(String authHeader, String dateHeader)
          throws S3Exception {
    if (authHeader == null || !authHeader.startsWith("AWS4")) {
      return null;
    }
    int algorithmPos = authHeader.indexOf(' ');
    if (algorithmPos < 0) {
      throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    //split the value parts of the authorization header
    String[] split = authHeader.substring(algorithmPos + 1).trim().split(", *");

    if (split.length != 3) {
      throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    String algorithm = authHeader.substring(0, algorithmPos);
    validateAlgorithm(authHeader, algorithm);
    AwsCredential credentialObj = parseCredentials(authHeader, split[0]);
    String signedHeaders = parseSignedHeaders(authHeader, split[1]);
    String signature = encodeSignature(authHeader, split[2]);
    return new SignatureInfo(
            SignatureInfo.Version.V4,
            credentialObj.getDate(),
            dateHeader,
            credentialObj.getAccessKeyID(),
            signature,
            signedHeaders,
            credentialObj.createScope(),
            algorithm,
            true
    );
  }

  /**
   * Validate signature.
   *
   * @param authHeader authorization header string
   * @param signature date string
   * @return encoded signature
   */
  private static String encodeSignature(String authHeader, String signature) throws S3Exception {
    if (signature.startsWith(SIGNATURE)) {
      String parsedSignature = signature.substring(SIGNATURE.length());
      if (StringUtils.isEmpty(parsedSignature)) {
        LOG.error("Signature can't be empty: {}", signature);
        throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
      try {
        Hex.decodeHex(parsedSignature);
      } catch (DecoderException e) {
        LOG.error("Signature:{} should be in hexa-decimal encoding.", signature);
        throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
      return parsedSignature;
    } else {
      LOG.error("No signature found: {}", signature);
      throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }

  /**
   * Validate credentials.
   *
   * @param authHeader authorization header string
   * @param credential credential
   * @return AwsCredential instance
   */
  private static AwsCredential parseCredentials(String authHeader, String credential)
          throws S3Exception {
    if (StringUtils.isNotEmpty(credential) && credential.startsWith(CREDENTIAL)) {
      credential = credential.substring(CREDENTIAL.length());
      // Parse credential. Other parts of header are not validated yet. When
      // security comes, it needs to be completed.
      return AwsCredential.create(credential);
    }
    throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
  }

  /**
   * Validate if algorithm is in expected format.
   */
  private static void validateAlgorithm(String authHeader, String algorithm) throws S3Exception {
    if (StringUtils.isEmpty(algorithm)
            || !algorithm.equals(AWS4_SIGNING_ALGORITHM)) {
      LOG.error("Unexpected hash algorithm. Algorithm:{}", algorithm);
      throw new S3Exception(authHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }
}
