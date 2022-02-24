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
import alluxio.stress.Parameters;
import alluxio.stress.TaskResult;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.common.AbstractMaxThroughputSummary;
import alluxio.stress.common.GeneralBenchSummary;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.worker.job.JobMasterClientContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * abstract class for MaxThroughput benchmark.
 *
 * @param <T> the MaxThroughput bench result summary
 * @param <S> the general Bench Summary with common method
 * @param <P> the stress bench parameter
 * @param <Q> the single task result
 */
public abstract class AbstractMaxThroughput<Q extends TaskResult, T extends
    AbstractMaxThroughputSummary<P, S>, S extends GeneralBenchSummary<Q>,
    P extends Parameters> extends Benchmark<T> {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractMaxThroughput.class);

  @ParametersDelegate
  protected P mParameters;

  protected T mMaxThroughputResult;

  protected int mInitialThroughput = -1;

  /**
   * Construct parameters with user command-line args.
   * @param baseArgs  initial args passed by the user
   */
  public abstract void initParameters(List<String> baseArgs);

  /**
   * Unit test for max throughput.
   * @return the results
   */
  protected abstract S runSingleTest(List<String> args, int targetThroughput) throws Exception;

  /**
   * Runs the test and returns the string output.
   *
   * @param args the command-line args
   * @return the string result output
   */
  public String run(String[] args) throws Exception {
    List<String> baseArgs = Lists.newArrayList();
    baseArgs.addAll(Arrays.asList(args));
    initParameters(baseArgs);
    JCommander jc = new JCommander(this);
    jc.setProgramName(this.getClass().getSimpleName());
    try {
      jc.parse(baseArgs.toArray(new String[0]));
      if (mBaseParameters.mHelp) {
        System.out.println(getBenchDescription());
        jc.usage();
        System.exit(0);
      }
    } catch (Exception e) {
      System.out.println(getBenchDescription());
      jc.usage();
      throw e;
    }
    // prepare the benchmark.
    prepare();
    T result = computeMaxThroughput(baseArgs);
    return result.toJson();
  }

  /**
   * Run multiple unit tests to find the max throughput.
   * @param baseArgs the user command-line args
   * @return the max throughput result
   */
  private T computeMaxThroughput(List<String> baseArgs) throws Exception {

    int numWorkers = 0;
    try (JobMasterClient client = JobMasterClient.Factory.create(
        JobMasterClientContext.newBuilder(ClientContext.create(new InstancedConfiguration(
            ConfigurationUtils.defaults()))).build())) {
      numWorkers = client.getAllWorkerHealth().size();
    }
    if (numWorkers <= 0) {
      throw new IllegalStateException("No workers available for testing!");
    }
    int lower = 0;
    int upper = Integer.MAX_VALUE;
    // use the input target throughput as the starting point
    int next = mInitialThroughput;
    int best = 0;
    while (true) {
      int perWorkerThroughput = next / numWorkers;
      int requestedThroughput = perWorkerThroughput * numWorkers;

      if (perWorkerThroughput == 0) {
        // Cannot run with a target of 0
        break;
      }

      List<String> newArgs = new ArrayList<>(baseArgs);
      updateArgValue(newArgs, "--target-throughput", Integer.toString(perWorkerThroughput));

      S mbr = runSingleTest(newArgs, perWorkerThroughput);

      int current = next;
      final float actualThroughput = mbr.getThroughput();
      if ((actualThroughput > requestedThroughput)
          || ((requestedThroughput - actualThroughput) / (float) requestedThroughput) < 0.02) {
        // the throughput was achieved. increase.
        mMaxThroughputResult.addPassedRun(current, mbr);

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
        mMaxThroughputResult.addFailedRun(current, mbr);

        upper = current;
        // throughput was not achieved. decrease.
        next = (lower + next) / 2;
      }
      LOG.info("target: " + requestedThroughput + " actual: " + actualThroughput + " [" + lower
          + " " + next + " " + upper + "]");
      for (String error : mbr.collectErrorsFromAllNodes()) {
        LOG.error("{}", error);
      }
      if (Math.abs(current - next) / (float) current <= 0.02) {
        break;
      }
    }
    mMaxThroughputResult.setEndTimeMs(CommonUtils.getCurrentMs());
    mMaxThroughputResult.setMaxThroughput(best);
    return mMaxThroughputResult;
  }

  protected void updateArgValue(List<String> args, String argName, String argValue) {
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

  @Override
  public T runLocal() {
    throw new UnsupportedOperationException("Not supported operation "
        + "when running maxThrough test.");
  }
}
