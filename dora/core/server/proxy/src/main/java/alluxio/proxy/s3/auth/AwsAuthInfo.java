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

package alluxio.proxy.s3.auth;

import com.google.common.base.MoreObjects;

/**
 * AWSAuthInfo wraps the data needed for AWS authentication.
 */
public class AwsAuthInfo {
  private final String mStringToSign;
  private final String mSignature;
  private final String mAccessID;

  /**
   *
   * @param stringToSign stringToSign
   * @param signature signature
   * @param accessID accessID
   */
  public AwsAuthInfo(String accessID, String stringToSign, String signature) {
    mAccessID = accessID;
    mStringToSign = stringToSign;
    mSignature = signature;
  }

  /**
   * @return StringTosSign
   */
  public String getStringTosSign() {
    return mStringToSign;
  }

  /**
   * @return signature
   */
  public String getSignature() {
    return mSignature;
  }

  /**
   * @return mAccessID
   */
  public String getAccessID() {
    return mAccessID;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("StringToSign", mStringToSign)
            .add("Signature", mSignature)
            .add("AccessID", mAccessID)
            .toString();
  }
}
