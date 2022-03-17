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

package alluxio.stress.cli;

import com.google.common.collect.ImmutableSet;

/**
 * A tool to run multiple tasks for StressBench.
 */
public class BatchTaskRunner {
  private static final ImmutableSet<String> MASTER_BATCH_TASK_NAMES = ImmutableSet.of(
      "MasterComprehensiveFileBatchTask");

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    if (!MASTER_BATCH_TASK_NAMES.contains(args[0])) {
      System.out.format("Unexpected batch task name: %s. Available options are: %s%n",
          args[0], MASTER_BATCH_TASK_NAMES);
      return;
    }
    BatchTask task = new MasterBatchTask();
    task.run(args);
  }
}
