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

import java.io.IOException;
import java.util.Map;

/**
 * Task for validating whether UFS directory is accessible.
 */
public final class UfsDirectoryValidationTask extends AbstractValidationTask {
  private final UnderFileSystem mUfs;
  private final String mPath;

  /**
   * Creates a new instance of {@link UfsDirectoryValidationTask}
   * for validating root under file system.
   */
  public UfsDirectoryValidationTask() {
    mUfs = UnderFileSystem.Factory.createForRoot();
    mPath = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
  }

  @Override
  public TaskResult validate(Map<String, String> optionsMap) {
    try {
      UfsStatus[] listStatus = mUfs.listStatus(mPath);
      if (listStatus == null) {
        System.err.format("Unable to list under file system path %s.%n", mPath);
        return TaskResult.FAILED;
      }

      return TaskResult.OK;
    } catch (IOException e) {
      System.err.format("Unable to access under file system path %s: %s.%n", mPath,
          e.getMessage());
      return TaskResult.FAILED;
    }
  }
}
