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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import static com.sun.tools.javac.util.Assert.checkNonNull;

/**
 * Credential in the AWS authorization header.
 * Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/
 * sigv4-auth-using-authorization-header.html
 *
 */
public class AwsCredential {
  private static final Logger LOG = LoggerFactory.getLogger(AwsCredential.class);

  private final String mAccessKeyID;
  private final String mDate;
  private final String mAwsRegion;
  private final String mAwsService;
  private final String mAwsRequest;

  AwsCredential(String accessKeyID, String date, String awsRegion, String awsService, String awsRequest) {
    mAccessKeyID = checkNonNull(accessKeyID, "AccessKeyID is null");
    mDate = checkNonNull(date, "Date is null");
    mAwsRegion = checkNonNull(awsRegion, "AwsRegion is null");
    mAwsService = checkNonNull(awsService, "AwsService is null");
    mAwsRequest = checkNonNull(awsRequest, "AwsRequest is null");
  }

  /**
   * Parse credential value.
   *
   * Sample credential value:
   * Credential=testuser/20220316/us-east-1/s3/aws4_request
   *
   * @throws S3Exception
   */

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
   * @return formatted scope string
   */
  public String createScope() {
    return String.format("%s/%s/%s/%s", getDate(),
       getAwsRegion(), getAwsService(),
       getAwsRequest());
  }

  static class Factory {
    /**
     * Parse credential value.
     *
     * Sample credential value:
     * Credential=testuser/20220316/us-east-1/s3/aws4_request
     *
     * @throws S3Exception
     */
    public static AwsCredential create(String credential) throws S3Exception {
      String[] split = credential.split("/");
      String accessKeyID, date, awsRegion, awsService, awsRequest;
      switch (split.length) {
        case 5:
          accessKeyID = split[0];
          date = split[1];
          awsRegion = split[2];
          awsService = split[3];
          awsRequest = split[4];
          break;
        case 6:
          // Access id is kerberos principal.
          accessKeyID  = String.format("%s/%s", split[0], split[1]);
          date = split[2];
          awsRegion = split[3];
          awsService = split[4];
          awsRequest = split[5];
          break;
        default:
          LOG.error("Credentials not in expected format. credential:{}", credential);
          throw new S3Exception(credential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
      validateCredential(credential, accessKeyID, date, awsRegion, awsRequest, date);
      return new AwsCredential(accessKeyID, date, awsRegion, awsService, awsRequest);
    }

    /**
     * validate credential info.
     * @throws S3Exception
     */
    public static void validateCredential(String credential, String accessKeyID, String awsRegion,
                                          String awsRequest, String awsService, String dateString) throws S3Exception {
      if (accessKeyID.isEmpty()) {
        LOG.error("Aws access id shouldn't be empty. credential:{}", credential);
        throw new S3Exception("Aws access id is empty", credential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
      if (awsRegion.isEmpty()) {
        LOG.error("Aws region shouldn't be empty. credential:{}", credential);
        throw new S3Exception("Aws region is empty", credential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
      if (awsRequest.isEmpty()) {
        LOG.error("Aws request shouldn't be empty. credential:{}", credential);
        throw new S3Exception("Aws request is empty", "credential", S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
      if (awsService.isEmpty()) {
        LOG.error("Aws service shouldn't be empty. credential:{}", credential);
        throw new S3Exception("Aws service is empty", credential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }

      // Date should not be empty and within valid range.
      if (!dateString.isEmpty()) {
        LocalDate date = LocalDate.parse(dateString, SignerConstants.DATE_FORMATTER);
        LocalDate now = LocalDate.now();
        if (date.isBefore(now.minus(1, ChronoUnit.DAYS))
                || date.isAfter(now.plus(1, ChronoUnit.DAYS))) {
          LOG.error("AWS date not in valid range. Date:{} should not be older "
                  + "than 1 day(i.e yesterday) and greater than 1 day(i.e "
                  + "tomorrow).", date);
          throw new S3Exception("AWS date not in valid range", credential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
        }
      } else {
        LOG.error("Aws date shouldn't be empty. credential:{}", credential);
        throw new S3Exception("Aws date is empty", credential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
    }
  }
}
