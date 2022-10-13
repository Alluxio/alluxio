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

package alluxio.cli.util;

import alluxio.client.job.JobMasterClient;
import alluxio.grpc.OperationType;
import alluxio.job.wire.CmdStatusBlock;
import alluxio.job.wire.SimpleJobStatusBlock;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The util class for command operations.
 */
public class DistributedCommandUtil {
  /**
   * Get detailed information about a command.
   * @param jobControlId the job control id of a command
   * @param client job master client
   * @param failedFiles all the failed files, stored in a set
   * @param completedFiles all the completed files
   */
  public static void getDetailedCmdStatus(
          long jobControlId, JobMasterClient client,
          Set<String> failedFiles,
          List<String> completedFiles) throws IOException {
    CmdStatusBlock cmdStatus = client.getCmdStatusDetailed(jobControlId);
    List<SimpleJobStatusBlock> blockList = cmdStatus.getJobStatusBlock();
    List<SimpleJobStatusBlock> failedBlocks = blockList.stream()
            .filter(block -> {
              String failed = block.getFilesPathFailed();
              if (failed.equals("") || failed == null) {
                return false;
              }
              return true;
            }).collect(Collectors.toList());

    failedBlocks.forEach(block -> {
      String[] files = block.getFilesPathFailed().split(",");
      failedFiles.addAll(Arrays.asList(files));
    });

    int failedCount = failedFiles.size();

    List<SimpleJobStatusBlock> nonEmptyPathBlocks = blockList.stream()
            .filter(block -> {
              String path = block.getFilePath();
              if (path.equals("") || path == null) {
                return false;
              }
              return true;
            }).collect(Collectors.toList());

    nonEmptyPathBlocks.stream().forEach(block -> {
      String[] paths = block.getFilePath().split(",");
      for (String path: paths) {
        if (!failedFiles.contains(path)) {
          completedFiles.add(path);
        }
      }
    });

    int completedCount = completedFiles.size();
    OperationType operationType = cmdStatus.getOperationType();
    String printKeyWord;
    if (operationType.equals(OperationType.DIST_LOAD)) {
      printKeyWord = "loaded";
    } else if (operationType.equals(OperationType.DIST_CP)) {
      printKeyWord = "copied";
    } else {
      printKeyWord = "processed";
    }

    if (cmdStatus.getJobStatusBlock().isEmpty()) {
      System.out.format("Job is completed successfully.%n");
    } else {
      System.out.format("Get command status information below: %n");

      completedFiles.forEach(file -> {
        System.out.format("Successfully %s path %s%n", printKeyWord, file);
      });

      System.out.format("Total completed file count is %s, failed file count is %s%n",
              completedCount, failedCount);
    }
  }
}
