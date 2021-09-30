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
import alluxio.stress.jobservice.JobServiceBenchParameters;
import alluxio.stress.jobservice.JobServiceBenchSummary;
import alluxio.stress.jobservice.JobServiceMaxThroughputSummary;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
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
public class JobServiceMaxThroughput extends Suite<JobServiceMaxThroughputSummary> {
  private static final Logger LOG = LoggerFactory.getLogger(JobServiceMaxThroughput.class);

  /** Reuse the existing parameters. */
  @ParametersDelegate
  private JobServiceBenchParameters mParameters = new JobServiceBenchParameters();

  private int mNumWorkers = 0;

  /**
   * @param args the command-line args
   */
  public static void main(String[] args) {
    mainInternal(args, new JobServiceMaxThroughput());
  }

  private JobServiceMaxThroughput() {}

  @Override
  public JobServiceMaxThroughputSummary runSuite(String[] args) throws Exception {
    try (JobMasterClient client = JobMasterClient.Factory.create(JobMasterClientContext
        .newBuilder(ClientContext.create(new InstancedConfiguration(ConfigurationUtils.defaults())))
        .build())) {
      mNumWorkers = client.getAllWorkerHealth().size();
    }
    if (mNumWorkers <= 0) {
      throw new IllegalStateException("No workers available for testing!");
    }

    JobServiceMaxThroughputSummary summary = new JobServiceMaxThroughputSummary();
    summary.setParameters(mParameters);
    List<String> baseArgs = new ArrayList<>(Arrays.asList(args));
    int best = getBestThroughput(mParameters.mTargetThroughput, summary, baseArgs);
    LOG.info("max throughput: " + best);

    summary.setEndTimeMs(CommonUtils.getCurrentMs());
    summary.setMaxThroughput(best);

    return summary;
  }

  // TODO(jianjian): possibly refactoring this function to a util with MaxThroughput
  private int getBestThroughput(int initialThroughput, JobServiceMaxThroughputSummary summary,
      List<String> baseArgs) throws Exception {
    int lower = 0;
    int upper = Integer.MAX_VALUE;
    // use the input target throughput as the starting point
    int next = initialThroughput;
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

      JobServiceBenchSummary mbr = runSingleTest(newArgs);

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
      LOG.info("target: " + requestedThroughput + " actual: " + actualThroughput + " [" + lower
          + " " + next + " " + upper + "]");
      for (Map.Entry<String, List<String>> entry : mbr.getErrors().entrySet()) {
        for (String error : entry.getValue()) {
          LOG.error(String.format("%s: %s", entry.getKey(), error));
        }
      }
      if (Math.abs(current - next) / (float) current <= 0.02) {
        break;
      }
    }
    return best;
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
   * @param args the args
   * @return the results
   */
  private JobServiceBenchSummary runSingleTest(List<String> args) throws Exception {
    Benchmark b = new StressMasterBench();
    String result = b.run(args.toArray(new String[0]));
    return JsonSerializable.fromJson(result, new JobServiceBenchSummary[0]);
  }
}
