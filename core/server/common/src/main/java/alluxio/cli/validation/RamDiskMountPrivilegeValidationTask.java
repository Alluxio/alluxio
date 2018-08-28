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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.OSUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Task for validating whether worker has enough privilege to mount RAM disk.
 */
public final class RamDiskMountPrivilegeValidationTask extends AbstractValidationTask {

  /**
   * Creates a new instance of {@link RamDiskMountPrivilegeValidationTask}
   * for validating mount privilege.
   */
  public RamDiskMountPrivilegeValidationTask() {
  }

  @Override
  public TaskResult validate(Map<String, String> optionsMap)
      throws InterruptedException {
    String path = Configuration.get(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH);
    String alias = Configuration.get(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS);
    if (!alias.equals("MEM")) {
      System.out.println("Top tier storage is not memory, skip validation.");
      return TaskResult.SKIPPED;
    }

    if (!OSUtils.isLinux()) {
      System.out.println("OS is not Linux, skip validation.");
      return TaskResult.SKIPPED;
    }

    if (path.isEmpty()) {
      System.err.println("Mount path is empty.");
      return TaskResult.FAILED;
    }

    if (path.split(",").length > 1) {
      System.out.println("Multiple storage paths for memory tier found. Skip validation.");
      return TaskResult.SKIPPED;
    }

    try {
      path = new AlluxioURI(path).getPath();
      File file = new File(path);
      if (file.exists()) {
        if (!file.isDirectory()) {
          System.err.format("Path %s is not a directory.%n", path);
          return TaskResult.FAILED;
        }

        if (Utils.isMountingPoint(path, new String[]{"ramfs", "tmpfs"})) {
          // If the RAM disk is already mounted, it must be writable for Alluxio worker
          // to be able to use it.
          if (!file.canWrite()) {
            System.err.format("RAM disk at %s is not writable.%n", path);
            return TaskResult.FAILED;
          }

          System.out.format("RAM disk is mounted at %s, skip validation.%n", path);
          return TaskResult.SKIPPED;
        }
      }

      // RAM disk is not mounted.
      // Makes sure Alluxio worker can mount ramfs by checking sudo privilege.
      if (!checkSudoPrivilege()) {
        System.err.println("No sudo privilege to mount ramfs. "
            + "If you would like to run Alluxio worker without sudo privilege, "
            + "please visit http://www.alluxio.org/docs/master/en/Running-Alluxio-Locally.html#"
            + "can-i-still-try-alluxio-on-linux-without-sudo-privileges");
        return TaskResult.FAILED;
      }
    } catch (IOException e) {
      System.err.format("Failed to validate ram disk mounting privilege at %s: %s.%n",
          path, e.getMessage());
      return TaskResult.FAILED;
    }

    return TaskResult.OK;
  }

  private boolean checkSudoPrivilege() throws InterruptedException, IOException {
    // "sudo -v" returns non-zero value if the current user has problem running sudo command.
    // It will always return zero if user is root.
    Process process = Runtime.getRuntime().exec("sudo -v");
    int exitCode = process.waitFor();
    return exitCode == 0;
  }
}
