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

import alluxio.stress.BaseParameters;
import alluxio.stress.master.MasterBatchTaskParameters;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Multiple tasks runner for Master StressBench.
 */
public class MasterBatchTask extends BatchTask {
  private static final Logger LOG = LoggerFactory.getLogger(MasterBatchTask.class);
  // Target throughput is set to a huge number to reach the maximum throughput of the task.
  private static final int TARGET_THROUGHPUT  = 1_000_000;

  @ParametersDelegate
  private BaseParameters mBaseParameter = new BaseParameters();
  @ParametersDelegate
  private MasterBatchTaskParameters mParameter = new MasterBatchTaskParameters();

  @Override
  public void run(String[] args) {
    JCommander jc = new JCommander(this);
    jc.setProgramName(this.getClass().getSimpleName());
    try {
      jc.parse(args);
    } catch (Exception e) {
      LOG.error("Failed to parse command: ", e);
      System.out.println(getDescription());
      throw e;
    }

    if (mParameter.mWriteType.equals("ALL")) {
      System.err.format("Parameter write-type ALL is not supported in batch task %s",
          mParameter.mTaskName);
      return;
    }

    if (mParameter.mStopCount != -1 && !(mParameter.mWarmup.equals("0")
        || mParameter.mWarmup.equals("0s"))) {
      System.out.println("Warning: when using stop-count parameter to claim the files "
          + "to be operated, the operations executed in warmup time will not be calculated in"
          + "the final result. The calculated operations will be less than expected.");
    }

    List<String[]> command = getCommand();
    for (String[] arg : command) {
      System.out.println("-----------------------------------------------------");
      System.out.format("Now executing command : %s on MasterStressBench...%n", arg[1]);
      // If one individual task fails, the pipeline will try to move on and finish
      try {
        StressMasterBench bench = new StressMasterBench();
        String jsonResult = bench.run(arg);
        System.out.println("Task finished successfully. The result is following :");
        System.out.println(jsonResult);
      } catch (Exception e) {
        System.err.format("Failed to finish the %s operation%n", arg[1]);
        e.printStackTrace();
      }
    }
    System.out.println("-----------------------------------------------------");
    System.out.println("All tasks finished. You can find the test results in the outputs above.");
  }

  private List<String[]> getCommand() {
    List<String[]> commandList = new ArrayList<>();

    if (mParameter.mTaskName.equals("MasterComprehensiveFileBatchTask")) {
      String[] operations = {"CreateFile", "ListDir", "ListDirLocated", "GetBlockLocations",
          "GetFileStatus", "OpenFile", "DeleteFile"};
      for (String op : operations) {
        List<String> command = new ArrayList<>(Arrays.asList(
            "--operation", op,
            "--base", mParameter.mBasePath,
            "--threads", String.valueOf(mParameter.mThreads),
            "--stop-count", String.valueOf(mParameter.mStopCount),
            "--target-throughput", String.valueOf(TARGET_THROUGHPUT),
            "--warmup", mParameter.mWarmup,
            "--create-file-size", mParameter.mCreateFileSize,
            "--write-type", mParameter.mWriteType,
            "--clients", String.valueOf(mParameter.mClients),
            "--client-type", mParameter.mClientType.toString(),
            "--read-type", mParameter.mReadType.toString(),
            "--fixed-count", String.valueOf(mParameter.mStopCount)
        ));

        // add all base parameters
        command.addAll(mBaseParameter.toBatchTaskArgumentString());

        commandList.add(command.toArray(new String[command.size()]));
      }
    }
    return commandList;
  }

  private String getDescription() {
    return String.join("\n", ImmutableList.of(
        "BatchTaskRunner is a tool to execute pre-defined group of MasterStressBench tasks",
        "",
        "Example:",
        "# this would run `CreateFile', 'ListDir', 'ListDirLocated', 'GetBlockLocations', "
            + "'GetFileStatus', 'OpenFile', 'DeleteFile' operations for 1000 files with size 1KB"
            + " in 10 threads and record the throughput. The file will be created in directory"
            + " alluxio:///stress-master-base",
        "$ bin/alluxio runClass alluxio.stress.cli.BatchTaskRunner"
            + " MasterComprehensiveFileBatchTask --num-files 1000 --threads 10 --create-file-size"
            + " 1k --base alluxio:///stress-master-base --warmup 0s",
        ""
    ));
  }
}
