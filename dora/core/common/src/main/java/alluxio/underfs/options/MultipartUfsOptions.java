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

package alluxio.underfs.options;

import alluxio.annotation.PublicApi;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for a MultipartUploading task in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public class MultipartUfsOptions {
  private byte[] mUfsCredential = null;

  /**
   * @return the Ufs Credential
   */
  public byte[] getUfsCredential() {
    return mUfsCredential;
  }

  /**
   * Sets the Ufs Credential.
   *
   * @param ufsCredential the UfsCredential
   * @return the updated CreateOptions
   */
  public MultipartUfsOptions setUfsCredential(byte[] ufsCredential) {
    if (ufsCredential != null) {
      mUfsCredential = ufsCredential.clone();
    }
    return this;
  }
}
