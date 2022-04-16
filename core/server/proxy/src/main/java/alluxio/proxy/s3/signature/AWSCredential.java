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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

/**
 * Credential in the AWS authorization header.
 * Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/
 * sigv4-auth-using-authorization-header.html
 *
 */
public class AWSCredential {
  private static final Logger LOG = LoggerFactory.getLogger(AWSCredential.class);

  private String mAccessKeyID;
  private String mDate;
  private String mAwsRegion;
  private String mAwsService;
  private String mAwsRequest;
  private String mCredential;

  /**
   * Construct Credential Object.
   * @param cred
   */
  AWSCredential(String cred) throws S3Exception {
    mCredential = cred;
    parseCredential();
    validateCredential();
  }

  /**
   * Parse credential value.
   *
   * Sample credential value:
   * Credential=testuser/20220316/us-east-1/s3/aws4_request
   *
   * @throws S3Exception
   */
  @SuppressWarnings("StringSplitter")
  public void parseCredential() throws S3Exception {
    String[] split = mCredential.split("/");
    switch (split.length) {
      case 5:
        mAccessKeyID = split[0].trim();
        mDate = split[1].trim();
        mAwsRegion = split[2].trim();
        mAwsService = split[3].trim();
        mAwsRequest = split[4].trim();
        return;
      case 6:
        // Access id is kerberos principal.
        mAccessKeyID = String.format("%s/%s",split[0], split[1]);
        mDate = split[2].trim();
        mAwsRegion = split[3].trim();
        mAwsService = split[4].trim();
        mAwsRequest = split[5].trim();
        return;
      default:
        LOG.error("Credentials not in expected format. credential:{}", mCredential);
        throw new S3Exception(mCredential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }

  /**
   * validate credential info.
   * @throws S3Exception
   */
  public void validateCredential() throws S3Exception {
    if (getAccessKeyID().isEmpty()) {
      LOG.error("AWS access id shouldn't be empty. credential:{}", mCredential);
      throw new S3Exception(mCredential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    if (getAwsRegion().isEmpty()) {
      LOG.error("AWS region shouldn't be empty. credential:{}", mCredential);
      throw new S3Exception(mCredential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    if (getAwsRequest().isEmpty()) {
      LOG.error("AWS request shouldn't be empty. credential:{}", mCredential);
      throw new S3Exception(mCredential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    if (getAwsService().isEmpty()) {
      LOG.error("AWS service shouldn't be empty. credential:{}", mCredential);
      throw new S3Exception(mCredential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    // Date should not be empty and within valid range.
    if (!getDate().isEmpty()) {
      validateDateRange();
    } else {
      LOG.error("AWS date shouldn't be empty. credential:{}", mCredential);
      throw new S3Exception(mCredential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }

  /**
   * validate date range.
   *
   * @throws S3Exception
   */
  public void validateDateRange() throws S3Exception {
    LocalDate date = LocalDate.parse(getDate(), SignerConstants.DATE_FORMATTER);
    LocalDate now = LocalDate.now();
    if (date.isBefore(now.minus(1, ChronoUnit.DAYS))
            || date.isAfter(now.plus(1, ChronoUnit.DAYS))) {
      LOG.error("AWS date not in valid range. Date:{} should not be older "
              + "than 1 day(i.e yesterday) and greater than 1 day(i.e "
              + "tomorrow).", date);
      throw new S3Exception(mCredential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
  }

  /**
   * @return AccessKeyID
   */
  public String getAccessKeyID() {
    return mAccessKeyID;
  }

  /**
   * @return date
   */
  public String getDate() {
    return mDate;
  }

  /**
   * @return region
   */
  public String getAwsRegion() {
    return mAwsRegion;
  }

  /**
   * @return service name
   */
  public String getAwsService() {
    return mAwsService;
  }

  /**
   * @return request info
   */
  public String getAwsRequest() {
    return mAwsRequest;
  }

  /**
   * @return credential
   */
  public String getCredential() {
    return mCredential;
  }

  /**
   * @return formatted scope string
   */
  public String createScope() {
    return String.format("%s/%s/%s/%s", getDate(),
       getAwsRegion(), getAwsService(),
       getAwsRequest());
  }
}
