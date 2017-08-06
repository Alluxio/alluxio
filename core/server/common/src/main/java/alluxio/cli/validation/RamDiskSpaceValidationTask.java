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
import alluxio.Constants;
import alluxio.PropertyKey;

import java.io.File;

/**
 * Task for validating whether worker RAM disk has enough space.
 */
public final class RamDiskSpaceValidationTask implements ValidationTask {

  /**
   * Creates a new instance of {@link RamDiskSpaceValidationTask}
   * for validating RAM disk size.
   */
  public RamDiskSpaceValidationTask() {
  }

  @Override
  public boolean validate() {
    long requiredSpace = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    String path = Configuration.get(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH);
    String alias = Configuration.get(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS);
    if (!alias.equals("MEM")) {
      System.out.println("Top tier storage is not memory, skip validation.");
      return true;
    }

    try {
      path = new AlluxioURI(path).getPath();
      File file = new File(path);
      if (!file.exists() || !file.isDirectory()) {
        System.out.format("RAM disk is not mounted at %s, skip validation.%n", path);
        return true;
      }

      long availableSpace = file.getTotalSpace();
      if (availableSpace < requiredSpace) {
        System.err.format(
            "Not enough space in RAM disk at location %s.%n"
            + "Required: %dMB; Available: %dMB.%n", path,
            requiredSpace / Constants.MB, availableSpace / Constants.MB);
        return false;
      }

      return true;
    } catch (SecurityException e) {
      System.err.format("Unable to access RAM disk at location %s.%n", path);
      return false;
    }
  }
}
