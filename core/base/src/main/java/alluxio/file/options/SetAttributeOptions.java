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

package alluxio.file.options;

import alluxio.Constants;
import alluxio.grpc.SetAttributePOptions;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class SetAttributeOptions {
  private SetAttributePOptions mOptions;
  private String mUfsFingerprint;

  public SetAttributeOptions(SetAttributePOptions protoOptions) {
    mOptions = protoOptions;
    setUfsFingerprint(Constants.INVALID_UFS_FINGERPRINT);
  }

  public SetAttributePOptions getOptions() {
    return mOptions;
  }
  /**
   * @return the ufs fingerprint
   */
  public String getUfsFingerprint() {
    return mUfsFingerprint;
  }

  /**
   * @param ufsFingerprint the ufs fingerprint
   * @return the updated options object
   */
  public SetAttributeOptions setUfsFingerprint(String ufsFingerprint) {
    mUfsFingerprint = ufsFingerprint;
    return this;
  }
}