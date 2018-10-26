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

package alluxio.cli.validation;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.UnderFileSystemUtils;

import com.google.common.base.Strings;

import java.io.IOException;
import java.util.Map;

/**
 * Task for validating whether Alluxio can access UFS as super user.
 */
public final class UfsSuperUserValidationTask extends AbstractValidationTask {
  private final UnderFileSystem mUfs;
  private final String mPath;

  /**
   * Creates a new instance of {@link UfsSuperUserValidationTask}
   * for validating root under file system.
   */
  public UfsSuperUserValidationTask() {
    mUfs = UnderFileSystem.Factory.createForRoot();
    mPath = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
  }

  @Override
  public TaskResult validate(Map<String, String> optionsMap) {
    if (!UnderFileSystemUtils.isHdfs(mUfs)) {
      // only support check on HDFS for now
      System.out.format("Under file system is not HDFS. Skip validation.%n");
      return TaskResult.SKIPPED;
    }
    UfsStatus status;
    try {
      status = mUfs.getStatus(mPath);
      if (status == null) {
        System.err.format("Unable to get status for under file system path %s.%n", mPath);
        return TaskResult.FAILED;
      }
      if (Strings.isNullOrEmpty(status.getOwner()) && Strings.isNullOrEmpty(status.getGroup())) {
        System.err.format("Cannot determine owner of under file system path %s.%n", mPath);
        return TaskResult.WARNING;
      }
    } catch (IOException e) {
      System.err.format("Unable to access under file system path %s: %s.%n", mPath,
          e.getMessage());
      return TaskResult.FAILED;
    }
    try {
      mUfs.setOwner(mPath, status.getOwner(), status.getGroup());
      return TaskResult.OK;
    } catch (IOException e) {
      System.err.format("Unable to set owner of under file system path %s: %s. "
          + "Please check if Alluxio is super user on the file system.%n", mPath, e.getMessage());
      return TaskResult.WARNING;
    }
  }
}
