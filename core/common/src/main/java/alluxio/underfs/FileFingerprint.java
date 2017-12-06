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
public class FileFingerprint extends Fingerprint {
  static final String FINGERPRINT_TYPE = "file";

  private static final String TAG_HASH = "hash";

  private static final String[] TAGS = {TAG_HASH};

  /**
   * Creates new instance of {@link FileFingerprint}.
   *
   * @param ufsName name of the ufs
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   * @param contentHash the hash of the contents
   */
  FileFingerprint(String ufsName, String owner, String group, String mode,
      String contentHash) {
    super(ufsName, FINGERPRINT_TYPE, owner, group, mode);
    putTag(TAG_HASH, contentHash);
  }

  FileFingerprint(Map<String, String> values) {
    super(values);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    for (String tag : TAGS) {
      sb.append(tag).append(':').append(getTag(tag)).append(' ');
    }
    return sb.toString();
  }
}
