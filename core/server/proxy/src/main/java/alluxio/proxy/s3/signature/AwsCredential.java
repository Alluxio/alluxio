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

import static alluxio.proxy.s3.signature.SignerConstants.DATE_FORMATTER;

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
public class AwsCredential {
  private static final Logger LOG = LoggerFactory.getLogger(AwsCredential.class);

  private final String mAccessKeyID;
  private final String mDate;
  private final String mAwsRegion;
  private final String mAwsService;
  private final String mAwsRequest;

  AwsCredential(String accessKeyID, String date, String awsRegion,
                String awsService, String awsRequest) {
    mAccessKeyID = accessKeyID;
    mDate = date;
    mAwsRegion = awsRegion;
    mAwsService = awsService;
    mAwsRequest = awsRequest;
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
      if (isValidCredential(credential)) {
        String[] split = credential.split("/");
        boolean isKerberos = isKerberosPrincipal(credential);
        String accessKeyID = isKerberos ? String.format("%s/%s", split[0], split[1]) : split[0];
        String date = isKerberos ? split[2] : split[1];
        String awsRegion = isKerberos ? split[3] : split[2];
        String awsService = isKerberos ? split[4] : split[3];
        String awsRequest = isKerberos ? split[5] : split[4];
        validateDateRange(credential, date);
        return new AwsCredential(accessKeyID, date, awsRegion, awsService, awsRequest);
      }
      LOG.error("Credentials not in expected format. credential:{}", credential);
      throw new S3Exception(credential, S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    private static boolean isValidCredential(String credential) {
      return credential.matches("(/\\S+){5,6}");
    }

    private static boolean isKerberosPrincipal(String credential) {
      return credential.matches("(/\\S+){6}");
    }

    /**
     * validate credential info.
     * @throws S3Exception
     */
    public static void validateDateRange(String credential, String dateString) throws S3Exception {
      // Date should not be empty and within valid range.
      if (!dateString.isEmpty()) {
        LocalDate date = LocalDate.parse(dateString, DATE_FORMATTER);
        LocalDate now = LocalDate.now();
        if (date.isBefore(now.minus(1, ChronoUnit.DAYS))
                || date.isAfter(now.plus(1, ChronoUnit.DAYS))) {
          LOG.error("AWS date not in valid range. Date:{} should not be older "
                  + "than 1 day(i.e yesterday) and greater than 1 day(i.e "
                  + "tomorrow).", date);
          throw new S3Exception("AWS date not in valid range", credential,
                  S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
        }
      } else {
        LOG.error("Aws date shouldn't be empty. credential:{}", credential);
        throw new S3Exception("Aws date is empty", credential,
                S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
      }
    }
  }
}
