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

import alluxio.client.job.JobGrpcClientUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.job.plan.PlanConfig;
import alluxio.job.wire.JobInfo;
import alluxio.stress.BaseParameters;
import alluxio.stress.TaskResult;
import alluxio.stress.job.StressBenchConfig;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class for all stress benchmarks.
 *
 * @param <T> the type of task result
 */
public abstract class Benchmark<T extends TaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

  @ParametersDelegate
  protected BaseParameters mBaseParameters = new BaseParameters();

  /**
   * Runs the test locally, in process.
   *
   * @return the task result
   */
  public abstract T runLocal() throws Exception;

  /**
   * Prepares to run the test.
   */
  public abstract void prepare() throws Exception;

  protected static void mainInternal(String[] args, Benchmark benchmark) {
    try {
      String result = benchmark.run(args);
      System.out.println(result);
      System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Generate a {@link StressBenchConfig} as the default JobConfig.
   *
   * @param args arguments
   * @return the JobConfig
   * */
  public PlanConfig generateJobConfig(String[] args) {
    // remove the cluster flag
    List<String> commandArgs =
            Arrays.stream(args).filter((s) -> !BaseParameters.CLUSTER_FLAG.equals(s))
                    .filter((s) -> !s.isEmpty()).collect(Collectors.toList());

    commandArgs.addAll(mBaseParameters.mJavaOpts);
    String className = this.getClass().getCanonicalName();
    return new StressBenchConfig(className, commandArgs, 10000, mBaseParameters.mClusterLimit);
  }

  /**
   * Runs the test and returns the string output.
   *
   * @param args the command-line args
   * @return the string result output
   */
  public String run(String[] args) throws Exception {
    JCommander jc = new JCommander(this);
    jc.setProgramName(this.getClass().getSimpleName());
    try {
      jc.parse(args);
      if (mBaseParameters.mHelp) {
        jc.usage();
        System.exit(0);
      }
    } catch (Exception e) {
      LOG.error("Failed to parse command: ", e);
      jc.usage();
      throw e;
    }

    // prepare the benchmark.
    prepare();

    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    String className = this.getClass().getCanonicalName();

    if (mBaseParameters.mCluster) {
      // run on job service
      long jobId =
          JobGrpcClientUtils.run(generateJobConfig(args), 0, conf);
      JobInfo jobInfo = JobGrpcClientUtils.getJobStatus(jobId, conf, true);
      return jobInfo.getResult().toString();
    }

    // run locally
    if (mBaseParameters.mInProcess) {
      LOG.debug("Run in process, mDistributed={}", mBaseParameters.mDistributed);

      // run in process
      T result = runLocal();
      if (mBaseParameters.mDistributed) {
        return result.toJson();
      }

      // aggregate the results
      final String s = result.aggregator().aggregate(Collections.singletonList(result)).toJson();
      return s;
    } else {
      // Spawn a new process
      List<String> command = new ArrayList<>();
      command.add(conf.get(PropertyKey.HOME) + "/bin/alluxio");
      command.add("runClass");
      command.add(className);
      command.addAll(Arrays.asList(args));
      command.add(BaseParameters.IN_PROCESS_FLAG);
      command.addAll(mBaseParameters.mJavaOpts);
      LOG.info("running command: " + String.join(" ", command));
      return ShellUtils.execCommand(command.toArray(new String[0]));
    }
  }
}
