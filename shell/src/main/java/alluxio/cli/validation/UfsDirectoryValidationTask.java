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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskRecoverEvent;

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
   *
   * @param conf configuration
   */
  public UfsDirectoryValidationTask(AlluxioConfiguration conf) {
    // TODO(jiacheng): UFSConf for nested path?
    mUfs = UnderFileSystem.Factory.createForRoot(conf);
    mPath = conf.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
  }

  @Override
  public TaskResult validate(Map<String, String> optionsMap) {
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();
    try {
      UfsStatus[] listStatus = mUfs.listStatus(mPath);
      if (listStatus == null) {
        msg.append(String.format("Unable to list under file system path %s. ", mPath));
        advice.append(String.format("Please check if path %s denotes a directory. ", mPath));
        return new TaskResult(State.FAILED, mName, msg.toString(), advice.toString());
      }
      msg.append(String.format("Successfully listed path %s. ", mPath));
      return new TaskResult(State.OK, mName, msg.toString(), advice.toString());
    } catch (IOException e) {
      msg.append(String.format("Unable to access under file system path %s: %s. ", mPath,
              e.getMessage()));
      TaskResult result = new TaskResult(State.FAILED, mName, msg.toString(), advice.toString());
      result.setError(e);
      return result;
    }
  }
}
