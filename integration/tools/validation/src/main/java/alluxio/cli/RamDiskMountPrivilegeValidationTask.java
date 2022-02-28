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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.OSUtils;
import alluxio.util.ShellUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Task for validating whether worker has enough privilege to mount RAM disk.
 */
public final class RamDiskMountPrivilegeValidationTask extends AbstractValidationTask {
  private final AlluxioConfiguration mConf;

  /**
   * Creates a new instance of {@link RamDiskMountPrivilegeValidationTask}
   * for validating mount privilege.
   * @param conf configuration
   */
  public RamDiskMountPrivilegeValidationTask(AlluxioConfiguration conf) {
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateRamDiskMountPrivilege";
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionsMap)
      throws InterruptedException {
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();

    String path = mConf.get(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH);
    String alias = mConf.get(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS);
    if (!alias.equals(Constants.MEDIUM_MEM)) {
      msg.append("Top tier storage is not memory, skip validation.");
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              msg.toString(), advice.toString());
    }

    if (!OSUtils.isLinux()) {
      msg.append("OS is not Linux, skip validation.");
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              msg.toString(), advice.toString());
    }

    if (path.isEmpty()) {
      msg.append(String.format("Mount path %s is empty.%n", path));
      advice.append(String.format("Please check your configuration %s=%s.%n",
              PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH.toString(), path));
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              msg.toString(), advice.toString());
    }

    if (path.split(",").length > 1) {
      msg.append("Multiple storage paths for memory tier found. Skip validation.");
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              msg.toString(), advice.toString());
    }

    try {
      path = new AlluxioURI(path).getPath();
      File file = new File(path);
      if (file.exists()) {
        if (!file.isDirectory()) {
          msg.append(String.format("Path %s is not a directory.%n", path));
          advice.append(String.format("Please check your path %s.%n", path));
          return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
                  msg.toString(), advice.toString());
        }

        if (ShellUtils.isMountingPoint(path, new String[]{"ramfs", "tmpfs"})) {
          // If the RAM disk is already mounted, it must be writable for Alluxio worker
          // to be able to use it.
          if (!file.canWrite()) {
            msg.append(String.format("RAM disk at %s is not writable.%n", path));
            advice.append("Please make sure the RAM disk path is writable");
            return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
                    msg.toString(), advice.toString());
          }

          msg.append(String.format("RAM disk is mounted at %s, skip validation.%n", path));
          return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
                  msg.toString(), advice.toString());
        }
      }

      // RAM disk is not mounted.
      // Makes sure Alluxio worker can mount ramfs by checking sudo privilege.
      if (!checkSudoPrivilege()) {
        msg.append("No sudo privilege to mount ramfs. ");
        advice.append("If you would like to run Alluxio worker without sudo privilege, "
            + "please visit https://docs.alluxio.io/os/user/stable/en/deploy/Running-Alluxio"
            + "-Locally.html#can-i-still-try-alluxio-on-linux-without-sudo-privileges");
        return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
                msg.toString(), advice.toString());
      }
    } catch (IOException e) {
      msg.append(String.format("Failed to validate ram disk mounting privilege at %s: %s.%n",
          path, e.getMessage()));
      msg.append(ValidationUtils.getErrorInfo(e));
      ValidationTaskResult result =
              new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
                      msg.toString(), advice.toString());
      return result;
    }

    msg.append("Worker has adequate privilege to mount RAM disk.");
    return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
            msg.toString(), advice.toString());
  }

  private boolean checkSudoPrivilege() throws InterruptedException, IOException {
    // "sudo -v" returns non-zero value if the current user has problem running sudo command.
    // It will always return zero if user is root.
    Process process = Runtime.getRuntime().exec("sudo -v");
    int exitCode = process.waitFor();
    return exitCode == 0;
  }
}
