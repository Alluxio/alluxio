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

package alluxio.underfs.b2;

import com.backblaze.b2.client.structures.B2Capabilities;

import java.util.Arrays;
import java.util.List;

/**
 * Util functions for B2 under file system.
 */
public final class B2Utils {

  private B2Utils() {} // Not intended for instantiation.

  /**
   * Translates B2 bucket ACL to Alluxio owner mode.
   *
   * @param capabilities the ACL list of B2 bucket
   * @return the translated posix mode in short format
   */
  public static short translateBucketAcl(List<String> capabilities) {
    short mode = (short) 0;

    if (capabilities
        .containsAll(Arrays.asList(B2Capabilities.READ_FILES, B2Capabilities.WRITE_FILES))) {
      // If the user has full control to the bucket, +rwx to the owner mode.
      mode |= (short) 0700;
    } else if (capabilities.contains(B2Capabilities.WRITE_FILES)) {
      // If the bucket is writable by the user, +w to the owner mode.
      mode |= (short) 0200;
    } else if (capabilities.contains(B2Capabilities.READ_FILES)) {
      // If the bucket is readable by the user, add r and x to the owner mode.
      mode |= (short) 0500;
    }

    return mode;
  }
}
