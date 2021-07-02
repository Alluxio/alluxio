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

package alluxio.cli;

import alluxio.conf.AlluxioConfiguration;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;

import java.util.Map;

/**
 * Task for validating whether UFS directory is accessible.
 */
@ApplicableUfsType(ApplicableUfsType.Type.ALL)
public final class UfsDirectoryValidationTask extends AbstractValidationTask {
  private final String mPath;
  private final AlluxioConfiguration mConf;

  /**
   * Creates a new instance of {@link UfsDirectoryValidationTask}
   * for validating root under file system.
   *
   * @param path the UFS path
   * @param conf the UFS configuration
   */
  public UfsDirectoryValidationTask(String path, AlluxioConfiguration conf) {
    mPath = path;
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateUfsDir";
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionsMap) {
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();
    try {
      UnderFileSystem ufs = UnderFileSystem.Factory.create(mPath, mConf);
      UfsStatus[] listStatus = ufs.listStatus(mPath);
      if (listStatus == null) {
        msg.append(String.format("Unable to list under file system path %s. ", mPath));
        advice.append(String.format("Please check if path %s denotes a directory. ", mPath));
        return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
                msg.toString(), advice.toString());
      }
      msg.append(String.format("Successfully listed path %s. ", mPath));
      return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
              msg.toString(), advice.toString());
    } catch (Exception e) {
      msg.append(String.format("Unable to access under file system path %s: %s. ", mPath,
              e.getMessage()));
      msg.append(ValidationUtils.getErrorInfo(e));
      advice.append(String.format("Please verify your path %s is correct.%n", mPath));
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              msg.toString(), advice.toString());
    }
  }
}
