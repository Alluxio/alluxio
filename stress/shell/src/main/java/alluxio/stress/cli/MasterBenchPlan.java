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
import alluxio.stress.master.MasterBenchPlanParameters;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Multiple tasks runner for Master StressBench.
 */
public class MasterBenchPlan extends BenchPlan {
  private static final Logger LOG = LoggerFactory.getLogger(MasterBenchPlan.class);
  private int mTargetThroughput = 1000000;
  private String mWarmUp = "0s";
  private String mDuration = "21600s";
  private String mCluster = "--cluster";

  private String mOperation;

  @ParametersDelegate
  private MasterBenchPlanParameters mParameter = new MasterBenchPlanParameters();

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
    try {
      for (String[] arg : command) {
        results.add(new StressMasterBench().runMultipleTask(arg));
      }
      output(results);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private List<String[]> getCommand() {
    List<String[]> commands = new ArrayList<>();

    if (mOperation.equals("MasterComprehensiveFileTestPlan")) {
      String[] operations = {"CreateFile", "ListDir", "ListDirLocated", "GetBlockLocations",
          "GetFileStatus", "OpenFile", "DeleteFile"};
      for (String op : operations) {
        commands.add(new String[] {
            "--operation", op,
            "--base", mParameter.mBasePath,
            "--threads", String.valueOf(mParameter.mThreads),
            "--stop-count", String.valueOf(mParameter.mAmount),
            "--target-throughput", String.valueOf(mTargetThroughput),
            "--warmup", mWarmUp,
            "--duration", mDuration,
            "--create-file-size", mParameter.mFileSize,
            mCluster,
        });
      }
    }
    else {
      String[] operations = {"CreateFile", "ListDir", "RenameFile", "ListDirLocated",
          "GetBlockLocations", "GetFileStatus", "OpenFile", "DeleteFile"};
      commands.add(new String[] {
          "--operation", "CreateFile",
          "--base", mParameter.mBasePath,
          "--threads", String.valueOf(mParameter.mThreads),
          "--stop-count", String.valueOf(mParameter.mAmount),
          "--target-throughput", String.valueOf(mTargetThroughput),
          "--warmup", mWarmUp,
          "--duration", mDuration,
          "--create-file-size", mParameter.mFileSize,
          mCluster,
      });
      commands.add(new String[] {
          "--operation", "",
          "--base", mParameter.mBasePath,
          "--threads", String.valueOf(mParameter.mThreads),
          "--stop-count", String.valueOf(mParameter.mAmount),
          "--target-throughput", String.valueOf(mTargetThroughput),
          "--warmup", mWarmUp,
          "--duration", mDuration,
          "--create-file-size", mParameter.mFileSize,
          mCluster,
      });
      for (String op : operations) {
        if (mOperation.contains(op)) {
          commands.get(1)[1] = op;
          break;
        }
      }
    }
    return commands;
  }

  private void output(List<Summary> results) throws Exception {
    for (Summary res : results) {
      System.out.println(res.toJson());
    }
  }
}
