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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringTokenizer;

/**
 * Class to parse v4 auth information from header.
 */
public class AuthorizationV4HeaderParser implements SignatureParser {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationV4HeaderParser.class);

  private static final String CREDENTIAL = "Credential=";
  private static final String SIGNEDHEADERS = "SignedHeaders=";
  private static final String SIGNATURE = "Signature=";
  private static final String AWS4_SIGNING_ALGORITHM = "AWS4-HMAC-SHA256";

  private final String mAuthHeader;

  private final String mDateHeader;

  /**
   * @param authHeader authorization header
   * @param dateHeader date header
   */
  public AuthorizationV4HeaderParser(String authHeader, String dateHeader) {
    mAuthHeader = authHeader;
    mDateHeader = dateHeader;
  }

  /**
   * Validate Signed headers.
   *
   * @param signedHeadersStr signed header
   * @return a string which parsed singed headers
   */
  private String parseSignedHeaders(String signedHeadersStr)
            throws S3Exception {
    if (StringUtils.isNotEmpty(signedHeadersStr)
         && signedHeadersStr.startsWith(SIGNEDHEADERS)) {
      String parsedSignedHeaders = signedHeadersStr.substring(SIGNEDHEADERS.length());
      StringTokenizer tokenizer = new StringTokenizer(parsedSignedHeaders, ";");
      if (tokenizer.hasMoreTokens()) {
        return parsedSignedHeaders;
      } else {
        LOG.error("No signed headers found. AuthHeader:{}", mAuthHeader);
        throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
    } else {
      LOG.error("No signed headers found. AuthHeader:{}", mAuthHeader);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }

  /**
   * This method parses authorization header.
   * @throws S3Exception
   */
  @Override
  public SignatureInfo parseSignature() throws S3Exception {
    if (mAuthHeader == null || !mAuthHeader.startsWith("AWS4")) {
      return null;
    }
    int algorithmPos = mAuthHeader.indexOf(' ');
    if (algorithmPos < 0) {
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    //split the value parts of the authorization header
    String[] split = mAuthHeader.substring(algorithmPos + 1).trim().split(", *");

    if (split.length != 3) {
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    String algorithm = mAuthHeader.substring(0, algorithmPos);
    validateAlgorithm(algorithm);
    AwsCredential credentialObj = parseCredentials(split[0]);
    String signedHeaders = parseSignedHeaders(split[1]);
    String signature = parseSignature(split[2]);
    return new SignatureInfo(
            SignatureInfo.Version.V4,
            credentialObj.getDate(),
            mDateHeader,
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
   */
  private String parseSignature(String signature) throws S3Exception {
    if (signature.startsWith(SIGNATURE)) {
      String parsedSignature = signature.substring(SIGNATURE.length());
      if (StringUtils.isEmpty(parsedSignature)) {
        LOG.error("Signature can't be empty: {}", signature);
        throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
      try {
        Hex.decodeHex(parsedSignature);
      } catch (DecoderException e) {
        LOG.error("Signature:{} should be in hexa-decimal encoding.", signature);
        throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
      return parsedSignature;
    } else {
      LOG.error("No signature found: {}", signature);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }

  /**
   * Validate credentials.
  */
  private AwsCredential parseCredentials(String credential) throws S3Exception {
    if (StringUtils.isNotEmpty(credential) && credential.startsWith(CREDENTIAL)) {
      credential = credential.substring(CREDENTIAL.length());
      // Parse credential. Other parts of header are not validated yet. When
      // security comes, it needs to be completed.
      return AwsCredential.Factory.create(credential);
    }
    throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
  }

  /**
   * Validate if algorithm is in expected format.
   */
  private void validateAlgorithm(String algorithm) throws S3Exception {
    if (StringUtils.isEmpty(algorithm)
            || !algorithm.equals(AWS4_SIGNING_ALGORITHM)) {
      LOG.error("Unexpected hash algorithm. Algo:{}", algorithm);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }
}
