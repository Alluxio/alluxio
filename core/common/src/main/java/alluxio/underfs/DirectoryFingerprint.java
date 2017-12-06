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

package alluxio.underfs;

import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Fingerprint for a UFS file.
 */
@NotThreadSafe
public class DirectoryFingerprint extends Fingerprint {
  static final String FINGERPRINT_TYPE = "directory";

  /**
   * Creates new instance of {@link DirectoryFingerprint}.
   *
   * @param ufsName name of the ufs
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   */
  DirectoryFingerprint(String ufsName, String owner, String group, String mode) {
    super(ufsName, FINGERPRINT_TYPE, owner, group, mode);
  }

  DirectoryFingerprint(Map<String, String> values) {
    super(values);
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
