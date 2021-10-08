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
import alluxio.stress.cli.StressJobServiceBench;
import alluxio.stress.jobservice.JobServiceBenchParameters;
import alluxio.stress.jobservice.JobServiceBenchSummary;
import alluxio.stress.jobservice.JobServiceMaxThroughputSummary;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.JsonSerializable;
import alluxio.worker.job.JobMasterClientContext;

import com.beust.jcommander.ParametersDelegate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A max throughput suite for job service.
 */
public class JobServiceMaxThroughput extends
    AbstractMaxThroughput<JobServiceMaxThroughputSummary, JobServiceBenchSummary,
        JobServiceBenchParameters> {

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
    int best = getBestThroughput(mParameters.mTargetThroughput, summary, baseArgs, mNumWorkers);
    LOG.info("max throughput: " + best);
    summary.setEndTimeMs(CommonUtils.getCurrentMs());
    summary.setMaxThroughput(best);
    return summary;
  }

  @Override
  protected JobServiceBenchSummary runSingleTest(List<String> args) throws Exception {
    Benchmark b = new StressJobServiceBench();
    String result = b.run(args.toArray(new String[0]));
    return JsonSerializable.fromJson(result, new JobServiceBenchSummary[0]);
  }
}
