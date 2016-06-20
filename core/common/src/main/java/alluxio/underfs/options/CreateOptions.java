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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.security.authorization.PermissionStatus;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public final class CreateOptions {
  // Permission status to set for the file being created.
  private PermissionStatus mPermissionStatus;
  // Block size in bytes for HDFS files only, not applicable for other Under FileSystems.
  private long mBlockSizeByte;

  /**
   * @return the default {@link CreateOptions}
   */
  public static CreateOptions defaults() {
    return new CreateOptions();
  }

  private CreateOptions() {
    mPermissionStatus = PermissionStatus.defaults();
    // By default HDFS block size is 64MB.
    mBlockSizeByte = 64 * Constants.MB;
  }

  /**
   * Constructs a {@link CreateOptions} with specified configuration.
   *
   * @param conf the configuration
   */
  public CreateOptions(Configuration conf) {
    // Only set the permission not the owner/group, because owner/group is not yet used for ufs
    // file creation.
    mPermissionStatus = PermissionStatus.defaults().applyFileUMask(conf);
    // By default HDFS block size is 64MB.
    mBlockSizeByte = 64 * Constants.MB;
  }

  /**
   * @return the block size in bytes
   */
  public long getBlockSizeByte() {
    return mBlockSizeByte;
  }

  /**
   * @return the permission status
   */
  public PermissionStatus getPermissionStatus() {
    return mPermissionStatus;
  }

  /**
   * Sets the block size.
   *
   * @param blockSizeByte the block size to set in bytes
   * @return the updated option object
   */
  public CreateOptions setBlockSizeByte(long blockSizeByte) {
    mBlockSizeByte = blockSizeByte;
    return this;
  }

  /**
   * Sets the permission status.
   *
   * @param permissionStatus the permission stats to set
   * @return the updated option object
   */
  public CreateOptions setPermissionStatus(PermissionStatus permissionStatus) {
    mPermissionStatus = permissionStatus;
    return this;
  }
}
