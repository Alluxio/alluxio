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

import alluxio.stress.Summary;
import alluxio.stress.master.MasterBatchTaskParameters;
import alluxio.util.JsonSerializable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Multiple tasks runner for Master StressBench.
 */
public class MasterBatchTask extends BatchTask {
  private static final Logger LOG = LoggerFactory.getLogger(MasterBatchTask.class);
  private static final int TARGETTHROUGHPUT = 1000000;
  private static final String WARMUP = "0s";
  private static final String DURATION = "21600s";
  private static final String CLUSTER = "--cluster";

  private String mOperation;

  @ParametersDelegate
  private MasterBatchTaskParameters mParameter = new MasterBatchTaskParameters();

  @Override
  public void run(String[] args) {
    mOperation = args[0];
    String[] input = Arrays.copyOfRange(args, 1, args.length);
    JCommander jc = new JCommander(this);
    jc.setProgramName(this.getClass().getSimpleName());
    try {
      jc.parse(input);
    } catch (Exception e) {
      LOG.error("Failed to parse command: ", e);
      jc.usage();
      throw e;
    }

    List<String[]> command = getCommand();
    List<Summary> results = new ArrayList<>();
    StressMasterBench bench = new StressMasterBench();
    for (String[] arg : command) {
      //error on individual task will not end the whole batch task
      try {
        String jsonResult = bench.run(arg);
        Summary result = (Summary) JsonSerializable.fromJson(jsonResult);
        results.add(result);
      } catch (Exception e) {
        System.out.println(String.format("Failed to finish the %s operation", arg[0]));
        e.printStackTrace();
      }
    }

    try {
      output(results);
    } catch (Exception e) {
      LOG.error("Failed to parse json: ", e);
      System.out.println("Failed to parse json");
      e.printStackTrace();
    }
  }

  private List<String[]> getCommand() {
    List<String[]> commands = new ArrayList<>();

    if (mOperation.equals("MasterComprehensiveFileBatchTask")) {
      String[] operations = {"CreateFile", "ListDir", "ListDirLocated", "GetBlockLocations",
          "GetFileStatus", "OpenFile", "DeleteFile"};
      for (String op : operations) {
        commands.add(new String[] {
            "--operation", op,
            "--base", mParameter.mBasePath,
            "--threads", String.valueOf(mParameter.mThreads),
            "--stop-count", String.valueOf(mParameter.mNumFiles),
            "--target-throughput", String.valueOf(TARGETTHROUGHPUT),
            "--warmup", WARMUP,
            "--duration", DURATION,
            "--create-file-size", mParameter.mFileSize,
            CLUSTER,
        });
      }
    } else {
      //store the possible operations
      Map<String, String> secondOperationMapping = ImmutableMap.<String, String>builder()
          .put("MasterOpenFileBatchTask", "OpenFile")
          .put("MasterGetBlockLocationsBatchTask", "GetBlockLocations")
          .put("MasterGetFileStatusBatchTask", "MasterGetFileStatus")
          .put("MasterRenameFileBatchTask", "RenameFile")
          .put("MasterListDirBatchTask", "ListDir")
          .put("MasterDeleteFileBatchTask", "DeleteFile")
          .build();

      //first command is creating the base file, second command depends on the task
      commands.add(new String[] {
          "--operation", "CreateFile",
          "--base", mParameter.mBasePath,
          "--threads", String.valueOf(mParameter.mThreads),
          "--stop-count", String.valueOf(mParameter.mNumFiles),
          "--target-throughput", String.valueOf(TARGETTHROUGHPUT),
          "--warmup", WARMUP,
          "--duration", DURATION,
          "--create-file-size", mParameter.mFileSize,
          CLUSTER,
      });
      commands.add(new String[] {
          "--operation", secondOperationMapping.get(mOperation),
          "--base", mParameter.mBasePath,
          "--threads", String.valueOf(mParameter.mThreads),
          "--stop-count", String.valueOf(mParameter.mNumFiles),
          "--target-throughput", String.valueOf(TARGETTHROUGHPUT),
          "--warmup", WARMUP,
          "--duration", DURATION,
          "--create-file-size", mParameter.mFileSize,
          CLUSTER,
      });
    }
    return commands;
  }

  private void output(List<Summary> results) throws Exception {
    for (Summary res : results) {
      System.out.println(res.toJson());
    }
  }
}
