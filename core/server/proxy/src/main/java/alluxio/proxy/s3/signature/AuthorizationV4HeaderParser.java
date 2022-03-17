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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import com.amazonaws.auth.internal.SignerConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Collection;
import java.time.temporal.ChronoUnit;

/**
 * Class to parse v4 auth information from header.
 */
public class AuthorizationV4HeaderParser implements SignatureParser {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationV4HeaderParser.class);

  private static final String CREDENTIAL = "Credential=";
  private static final String SIGNEDHEADERS = "SignedHeaders=";
  private static final String SIGNATURE = "Signature=";

  private String mAuthHeader;

  private String mDateHeader;

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
      Collection<String> signedHeaders =
              alluxio.util.StringUtils.getStringCollection(parsedSignedHeaders, ";");
      if (signedHeaders.size() == 0) {
        LOG.error("No signed headers found. Authheader:{}", mAuthHeader);
        throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
      return parsedSignedHeaders;
    } else {
      LOG.error("No signed headers found. Authheader:{}", mAuthHeader);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }

  /**
   * This method parses authorization header.
   * <p>
   * Authorization Header sample:
   * AWS4-HMAC-SHA256 Credential=AKIAJWFJK62WUTKNFJJA/20181009/us-east-1/s3
   * /aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date,
   * Signature
   * =db81b057718d7c1b3b8dffa29933099551c51d787b3b13b9e0f9ebed45982bf2
   *
   * @throws S3Exception
   */
  @SuppressWarnings("StringSplitter")
  @Override
  public SignatureInfo parseSignature() throws S3Exception {
    if (mAuthHeader == null || !mAuthHeader.startsWith("AWS4")) {
      return null;
    }
    int firstSep = mAuthHeader.indexOf(' ');
    if (firstSep < 0) {
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    //split the value parts of the authorization header
    String[] split = mAuthHeader.substring(firstSep + 1).trim().split(", *");

    if (split.length != 3) {
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    String algorithm = parseAlgorithm(mAuthHeader.substring(0, firstSep));
    Credential credentialObj = parseCredentials(split[0]);
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
  private Credential parseCredentials(String credential) throws S3Exception {
    Credential credentialObj = null;
    if (StringUtils.isNotEmpty(credential) && credential.startsWith(CREDENTIAL)) {
      credential = credential.substring(CREDENTIAL.length());
      // Parse credential. Other parts of header are not validated yet. When
      // security comes, it needs to be completed.
      credentialObj = new Credential(credential);
    } else {
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    if (credentialObj.getAccessKeyID().isEmpty()) {
      LOG.error("AWS access id shouldn't be empty. credential:{}", credential);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    if (credentialObj.getAwsRegion().isEmpty()) {
      LOG.error("AWS region shouldn't be empty. credential:{}", credential);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    if (credentialObj.getAwsRequest().isEmpty()) {
      LOG.error("AWS request shouldn't be empty. credential:{}", credential);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    if (credentialObj.getAwsService().isEmpty()) {
      LOG.error("AWS service shouldn't be empty. credential:{}", credential);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    // Date should not be empty and within valid range.
    if (!credentialObj.getDate().isEmpty()) {
      validateDateRange(credentialObj);
    } else {
      LOG.error("AWS date shouldn't be empty. credential:{}", credential);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    return credentialObj;
  }

  /**
   * validate date range.
   *
   * @param credentialObj
   * @throws S3Exception
   */
  @VisibleForTesting
  public void validateDateRange(Credential credentialObj) throws S3Exception {
    LocalDate date = LocalDate.parse(credentialObj.getDate(), SignatureProcessor.DATE_FORMATTER);
    LocalDate now = LocalDate.now();
    if (date.isBefore(now.minus(1, ChronoUnit.DAYS))
        || date.isAfter(now.plus(1, ChronoUnit.DAYS))) {
      LOG.error("AWS date not in valid range. Date:{} should not be older "
              + "than 1 day(i.e yesterday) and greater than 1 day(i.e "
              + "tomorrow).", date);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }

  /**
   * Validate if algorithm is in expected format.
   */
  private String parseAlgorithm(String algorithm) throws S3Exception {
    if (StringUtils.isEmpty(algorithm)
            || !algorithm.equals(SignerConstants.AWS4_SIGNING_ALGORITHM)) {
      LOG.error("Unexpected hash algorithm. Algo:{}", algorithm);
      throw new S3Exception(mAuthHeader, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    return algorithm;
  }
}
