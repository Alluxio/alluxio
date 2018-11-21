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

package alluxio.wire;

import alluxio.grpc.BackupPOptions;

import javax.annotation.Nullable;

/**
 * Options for backing up the Alluxio master.
 */
public class BackupOptions {
  @Nullable
  private String mTargetDirectory;
  private boolean mLocalFileSystem;

  /**
   * @param targetDirectory the directory to write a backup to; null means to use the default backup
   *        directory
   * @param localFileSystem whether to write to the local filesystem instead of the root UFS
   */
  public BackupOptions(@Nullable String targetDirectory, boolean localFileSystem) {
    mTargetDirectory = targetDirectory;
    mLocalFileSystem = localFileSystem;
  }

  /**
   * @param pOpts proto options
   * @return wire type options corresponding to the proto options
   */
  public static BackupOptions fromProto(BackupPOptions pOpts) {
    return new BackupOptions(pOpts.getTargetDirectory(), pOpts.getLocalFileSystem());
  }

  /**
   * @return the proto options corresponding to these options
   */
  public BackupPOptions toProto() {
    return BackupPOptions.newBuilder().setTargetDirectory(mTargetDirectory)
        .setLocalFileSystem(mLocalFileSystem).build();
  }

  /**
   * @return the directory to write a backup to; null means to use the default backup directory
   */
  @Nullable
  public String getTargetDirectory() {
    return mTargetDirectory;
  }

  /**
   * @return whether to write to the local filesystem instead of the root UFS
   */
  public boolean isLocalFileSystem() {
    return mLocalFileSystem;
  }
}
