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

/**
 * Signature and related information.
 * <p>
 * Required to create stringToSign and token.
 */
public class SignatureInfo {
  private final Version mVersion;

  /**
   * Information comes from the credential (Date only).
   */
  private final String mDate;

  /**
   * Information comes from header/query param (full timestamp).
   */
  private final String mDateTime;

  private final String mAwsAccessId;

  private final String mSignature;

  private final String mSignedHeaders;

  private final String mCredentialScope;

  private final String mAlgorithm;

  private final boolean mSignPayload;

  /**
   * Constructs a new {@link SignatureInfo}.
   *
   * @param version
   * @param date
   * @param dateTime
   * @param awsAccessId
   * @param signature
   * @param signedHeaders
   * @param credentialScope
   * @param algorithm
   * @param signPayload
   */
  public SignatureInfo(
      Version version,
      String date,
      String dateTime,
      String awsAccessId,
      String signature,
      String signedHeaders,
      String credentialScope,
      String algorithm,
      boolean signPayload
  ) {
    mVersion = version;
    mDate = date;
    mDateTime = dateTime;
    mAwsAccessId = awsAccessId;
    mSignature = signature;
    mSignedHeaders = signedHeaders;
    mCredentialScope = credentialScope;
    mAlgorithm = algorithm;
    mSignPayload = signPayload;
  }

  /**
   * @return AccessId
   */
  public String getAwsAccessId() {
    return mAwsAccessId;
  }

  /**
   *
   * @return signature
   */
  public String getSignature() {
    return mSignature;
  }

  /**
   * @return date
   */
  public String getDate() {
    return mDate;
  }

  /**
   * @return signed headers string
   */
  public String getSignedHeaders() {
    return mSignedHeaders;
  }

  /**
   * @return credential scope
   */
  public String getCredentialScope() {
    return mCredentialScope;
  }

  /**
   * @return algorithm
   */
  public String getAlgorithm() {
    return mAlgorithm;
  }

  /**
   * @return version
   */
  public Version getVersion() {
    return mVersion;
  }

  /**
   * @return whether is SignPayload
   */
  public boolean isSignPayload() {
    return mSignPayload;
  }

  /**
   * @return dateTime string
   */
  public String getDateTime() {
    return mDateTime;
  }

  /**
   * Signature version.
   */
  public enum Version {
    V4, V2;
  }
}
