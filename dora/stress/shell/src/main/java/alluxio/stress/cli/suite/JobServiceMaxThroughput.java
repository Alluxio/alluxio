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

import alluxio.job.util.SerializationUtils;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.cli.StressJobServiceBench;
import alluxio.stress.common.GeneralBenchSummary;
import alluxio.stress.jobservice.JobServiceBenchParameters;
import alluxio.stress.jobservice.JobServiceBenchSummary;
import alluxio.stress.jobservice.JobServiceBenchTaskResult;
import alluxio.stress.jobservice.JobServiceMaxThroughputSummary;
import alluxio.util.JsonSerializable;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A max throughput suite for job service.
 */
public class JobServiceMaxThroughput extends
    AbstractMaxThroughput<JobServiceBenchTaskResult, JobServiceMaxThroughputSummary,
        GeneralBenchSummary<JobServiceBenchTaskResult>, JobServiceBenchParameters> {
  /**
   * @param args the command-line args
   */
  public static void main(String[] args) {
    mainInternal(args, new JobServiceMaxThroughput());
  }

  private JobServiceMaxThroughput() {}

  @Override
  public void initParameters(List<String> baseArgs) {
    mParameters = new JobServiceBenchParameters();
  }

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "",
        "A benchmarking tool to measure the job service max throughput of Alluxio.",
        "Example:",
        "# this would continuously run `CreateFiles` opeartion and record the throughput.",
        "$ bin/alluxio runClass alluxio.stress.cli.suite.JobServiceMaxThroughput --operation Noop",
        ""
    ));
  }

  @Override
  protected JobServiceBenchSummary runSingleTest(List<String> args,
      int targetThroughput) throws Exception {
    Benchmark b = new StressJobServiceBench();
    String result = b.run(args.toArray(new String[0]));
    return JsonSerializable.fromJson(
        SerializationUtils.parseBenchmarkResult(result), new JobServiceBenchSummary[0]);
  }

  @Override
  public void prepare() {
    mMaxThroughputResult = new JobServiceMaxThroughputSummary();
    mMaxThroughputResult.setParameters(mParameters);
    mInitialThroughput = mParameters.mTargetThroughput;
  }
}
