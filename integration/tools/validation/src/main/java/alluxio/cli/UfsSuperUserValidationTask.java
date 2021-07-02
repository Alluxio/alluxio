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

import com.google.common.base.Strings;

import java.io.IOException;
import java.util.Map;

/**
 * Task for validating whether Alluxio can access UFS as super user.
 */
@ApplicableUfsType(ApplicableUfsType.Type.HDFS)
public final class UfsSuperUserValidationTask extends AbstractValidationTask {
  private final AlluxioConfiguration mConf;
  private final String mPath;

  /**
   * Creates a new instance of {@link UfsSuperUserValidationTask}
   * for validating the under file system.
   *
   * @param path the UFS path
   * @param conf the UFS configuration
   */
  public UfsSuperUserValidationTask(String path, AlluxioConfiguration conf) {
    mPath = path;
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateSuperUserPrivilege";
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionsMap) {
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();

    if (!ValidationUtils.isHdfsScheme(mPath)) {
      // only support check on HDFS for now
      msg.append(String.format("Under file system is not HDFS. Skip validation. "));
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              msg.toString(), advice.toString());
    }
    UfsStatus status;
    UnderFileSystem ufs;
    try {
      ufs = UnderFileSystem.Factory.create(mPath, mConf);
      status = ufs.getStatus(mPath);
      if (status == null) {
        msg.append(String.format("Unable to get status for under file system path %s. ", mPath));
        advice.append(String.format("Please check your path %s. ", mPath));
        return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
                msg.toString(), advice.toString());
      }
      if (Strings.isNullOrEmpty(status.getOwner()) && Strings.isNullOrEmpty(status.getGroup())) {
        msg.append(String.format("Cannot determine owner of under file system path %s. ", mPath));
        advice.append(String.format("Please check your path %s. ", mPath));
        return new ValidationTaskResult(ValidationUtils.State.WARNING, getName(),
                msg.toString(), advice.toString());
      }
    } catch (Exception e) {
      msg.append(String.format("Unable to access under file system path %s: %s.", mPath,
          e.getMessage()));
      msg.append(ValidationUtils.getErrorInfo(e));
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              msg.toString(), advice.toString());
    }
    try {
      ufs.setOwner(mPath, status.getOwner(), status.getGroup());
      msg.append(String.format("User has superuser privilege to path %s.%n", mPath));
      return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
              msg.toString(), advice.toString());
    } catch (IOException e) {
      msg.append(String.format("Unable to set owner of under file system path %s: %s. ",
              mPath, e.getMessage()));
      advice.append("Please check if Alluxio is super user on the file system. ");
      return new ValidationTaskResult(ValidationUtils.State.WARNING, getName(),
              msg.toString(), advice.toString());
    }
  }
}
