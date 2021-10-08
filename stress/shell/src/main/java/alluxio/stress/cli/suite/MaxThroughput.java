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

package alluxio.stress.cli.suite;

import alluxio.ClientContext;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.cli.StressMasterBench;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.stress.master.MasterBenchSummary;
import alluxio.stress.master.MaxThroughputSummary;
import alluxio.stress.master.Operation;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.JsonSerializable;
import alluxio.worker.job.JobMasterClientContext;

import com.beust.jcommander.ParametersDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A max throughput suite.
 */
public class MaxThroughput extends Suite<MaxThroughputSummary> {
  private static final Logger LOG = LoggerFactory.getLogger(MaxThroughput.class);

  /** Reuse the existing parameters. */
  @ParametersDelegate
  private MasterBenchParameters mParameters = new MasterBenchParameters();

  private int mNumWorkers = 0;

  /**
   * @param args the command-line args
   */
  public static void main(String[] args) {
    mainInternal(args, new MaxThroughput());
  }

  private MaxThroughput() {
  }

  @Override
  public MaxThroughputSummary runSuite(String[] args) throws Exception {
    try (JobMasterClient client = JobMasterClient.Factory.create(
        JobMasterClientContext.newBuilder(ClientContext.create(new InstancedConfiguration(
            ConfigurationUtils.defaults()))).build())) {
      mNumWorkers = client.getAllWorkerHealth().size();
    }
    if (mNumWorkers <= 0) {
      throw new IllegalStateException("No workers available for testing!");
    }

    MaxThroughputSummary summary = new MaxThroughputSummary();
    summary.setParameters(mParameters);

    List<String> baseArgs = new ArrayList<>(Arrays.asList(args));

    if (!mParameters.mSkipPrepare) {
      prepareBeforeAllTests(baseArgs);
    }

    int lower = 0;
    int upper = Integer.MAX_VALUE;
    // use the input target throughput as the starting point
    int next = mParameters.mTargetThroughput;
    int best = 0;

    while (true) {
      int perWorkerThroughput = next / mNumWorkers;
      int requestedThroughput = perWorkerThroughput * mNumWorkers;

      if (perWorkerThroughput == 0) {
        // Cannot run with a target of 0
        break;
      }

      List<String> newArgs = new ArrayList<>(baseArgs);
      updateArgValue(newArgs, "--target-throughput", Integer.toString(perWorkerThroughput));

      long runSec = (FormatUtils.parseTimeSize(mParameters.mDuration) + FormatUtils
          .parseTimeSize(mParameters.mWarmup)) / 1000;
      // the expected number of paths required for the test to complete successfully
      long requiredCount = next * runSec;

      MasterBenchSummary mbr = runSingleTest(requiredCount, newArgs);

      int current = next;
      final float actualThroughput = mbr.getThroughput();
      if ((actualThroughput > requestedThroughput)
          || ((requestedThroughput - actualThroughput) / (float) requestedThroughput) < 0.02) {
        // the throughput was achieved. increase.
        summary.addPassedRun(current, mbr);

        best = current;
        // update the lower bound.
        lower = current;

        if (upper == Integer.MAX_VALUE) {
          next *= 2;
        } else {
          next = (next + upper) / 2;
        }
      } else {
        // Failed to achieve the target throughput. update the upper bound.
        summary.addFailedRun(current, mbr);

        upper = current;
        // throughput was not achieved. decrease.
        next = (lower + next) / 2;
      }
      LOG.info(
          "target: " + requestedThroughput + " actual: " + actualThroughput + " [" + lower + " "
              + next + " " + upper + "]");
      for (Map.Entry<String, List<String>> entry : mbr.getErrors().entrySet()) {
        for (String error : entry.getValue()) {
          LOG.error(String.format("%s: %s", entry.getKey(), error));
        }
      }
      if (Math.abs(current - next) / (float) current <= 0.02) {
        break;
      }
    }
    LOG.info("max throughput: " + best);

    summary.setEndTimeMs(CommonUtils.getCurrentMs());
    summary.setMaxThroughput(best);

    return summary;
  }

  private void updateArgValue(List<String> args, String argName, String argValue) {
    int index = args.indexOf(argName);
    if (index == -1) {
      // arg not found
      args.add(argName);
      args.add(argValue);
      return;
    }
    if (index + 1 < args.size()) {
      // arg found and next index is valid
      args.set(index + 1, argValue);
    } else {
      // the next index is out of bounds
    }
  }

  /**
   * @param numFiles number of files to create with each task
   * @param args the args
   */
  private void createFiles(long numFiles, List<String> args) throws Exception {
    List<String> newArgs = new ArrayList<>(args);
    updateArgValue(newArgs, "--operation", Operation.CREATE_FILE.toString());
    updateArgValue(newArgs, "--warmup", "0s");
    updateArgValue(newArgs, "--threads", "128");
    updateArgValue(newArgs, "--stop-count", Long.toString(numFiles));
    updateArgValue(newArgs, "--target-throughput", "10000");

    LOG.info(String.format("Preparing %d files. args: %s", numFiles, String.join(" ", newArgs)));

    Benchmark b = new StressMasterBench();
    String result = b.run(newArgs.toArray(new String[0]));
    MasterBenchSummary summary = JsonSerializable.fromJson(result, new MasterBenchSummary[0]);
    if (!summary.getErrors().isEmpty()) {
      throw new IllegalStateException(String
          .format("Could not create files for operation (%s). error: %s",
              mParameters.mOperation, summary.getErrors().entrySet().iterator().next()));
    }
  }

  private void prepareBeforeAllTests(List<String> args) throws Exception {
    switch (mParameters.mOperation) {
      case GET_BLOCK_LOCATIONS: // initial state requires createFile
      case GET_FILE_STATUS:     // initial state requires createFile
      case LIST_DIR:           // initial state requires createFile
      case LIST_DIR_LOCATED:    // initial state requires createFile
      case OPEN_FILE:          // initial state requires createFile
        createFiles(mParameters.mFixedCount, args);
        break;
      case CREATE_FILE: // do nothing, since creates do not need initial state
      case CREATE_DIR:  // do nothing, since creates do not need initial state
      case RENAME_FILE: // do nothing, since creates will happen before each test run
      case DELETE_FILE: // do nothing, since creates will happen before each test run
      default:
        break;
    }
  }

  /**
   * @param requiredCount the number of operations that may happen for a successful run
   * @param args the args
   * @return the results
   */
  private void prepareBeforeSingleTest(long requiredCount, List<String> args) throws Exception {
    switch (mParameters.mOperation) {
      case RENAME_FILE: // prepare files
      case DELETE_FILE: // prepare files
        // create an extra buffer of created files
        float perWorkerCount = (float) requiredCount / mNumWorkers * 1.5f;
        createFiles(Math.max((long) perWorkerCount, mParameters.mFixedCount), args);
        break;
      case CREATE_FILE:        // do nothing
      case GET_BLOCK_LOCATIONS: // do nothing
      case GET_FILE_STATUS:     // do nothing
      case LIST_DIR:           // do nothing
      case LIST_DIR_LOCATED:    // do nothing
      case OPEN_FILE:          // do nothing
      case CREATE_DIR:         // do nothing
      default:
        break;
    }
  }

  /**
   * @param requiredCount the number of operations that may happen for a successful run
   * @param args the args
   * @return the results
   */
  private MasterBenchSummary runSingleTest(long requiredCount, List<String> args) throws Exception {
    prepareBeforeSingleTest(requiredCount, args);

    Benchmark b = new StressMasterBench();
    String result = b.run(args.toArray(new String[0]));
    return JsonSerializable.fromJson(result, new MasterBenchSummary[0]);
  }
}
