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
import alluxio.job.util.SerializationUtils;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.cli.StressMasterBench;
import alluxio.stress.common.GeneralBenchSummary;
import alluxio.stress.master.MasterBenchSummary;
import alluxio.stress.master.MasterBenchTaskResult;
import alluxio.stress.master.MasterMaxThroughputSummary;
import alluxio.stress.master.Operation;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.util.FormatUtils;
import alluxio.util.JsonSerializable;
import alluxio.worker.job.JobMasterClientContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A max throughput suite for master.
 */
public class MasterMaxThroughput extends
    AbstractMaxThroughput<MasterBenchTaskResult, MasterMaxThroughputSummary,
        GeneralBenchSummary<MasterBenchTaskResult>, MasterBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(MasterMaxThroughput.class);

  private int mNumWorkers = 0;

  /**
   * Command-line args passed by user. Don't modify it.
   */
  private final List<String> mBaseArgs = Lists.newArrayList();

  /**
   * @param args the command-line args
   */
  public static void main(String[] args) {
    mainInternal(args, new MasterMaxThroughput());
  }

  private MasterMaxThroughput() {
  }

  @Override
  public void initParameters(List<String> baseArgs) {
    mParameters = new MasterBenchParameters();
    mBaseArgs.addAll(baseArgs);
  }

  @Override
  public void prepare() throws Exception {
    mMaxThroughputResult = new MasterMaxThroughputSummary();
    mMaxThroughputResult.setParameters(mParameters);
    mInitialThroughput = mParameters.mTargetThroughput;
    if (!mParameters.mSkipPrepare) {
      prepareBeforeAllTests(mBaseArgs);
    }
    try (JobMasterClient client = JobMasterClient.Factory.create(
        JobMasterClientContext.newBuilder(ClientContext.create()).build())) {
      mNumWorkers = client.getAllWorkerHealth().size();
    }
  }

  @Override
  protected MasterBenchSummary runSingleTest(List<String> args,
      int targetThroughput) throws Exception {
    long runSec = (FormatUtils.parseTimeSize(mParameters.mDuration) + FormatUtils
        .parseTimeSize(mParameters.mWarmup)) / 1000;
    // the expected number of paths required for the test to complete successfully
    long requiredCount = targetThroughput * runSec;
    prepareBeforeSingleTest(requiredCount, args);
    Benchmark b = new StressMasterBench();
    String result = b.run(args.toArray(new String[0]));
    return JsonSerializable.fromJson(
        SerializationUtils.parseBenchmarkResult(result), new MasterBenchSummary[0]);
  }

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "",
        "A benchmarking tool to measure the master max throughput of Alluxio.",
        "Example:",
        "# this would continuously run `ListDir` opeartion and record the throughput after "
            + "5s warmup.",
        "$ bin/alluxio runClass alluxio.stress.cli.suite.MasterMaxThroughput \\",
        "--operation ListDir --warmup 5s",
        ""
    ));
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
    MasterBenchSummary summary = JsonSerializable.fromJson(
        SerializationUtils.parseBenchmarkResult(result), new MasterBenchSummary[0]);
    if (!summary.collectErrorsFromAllNodes().isEmpty()) {
      throw new IllegalStateException(String
          .format("Could not create files for operation (%s). error: %s",
              mParameters.mOperation, summary.collectErrorsFromAllNodes().iterator().next()));
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
}
