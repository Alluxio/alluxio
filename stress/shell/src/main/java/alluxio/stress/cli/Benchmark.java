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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.client.job.JobGrpcClientUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.job.plan.PlanConfig;
import alluxio.job.wire.JobInfo;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.TaskResult;
import alluxio.stress.job.StressBenchConfig;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.ShellUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
   * Get the description of the bench.
   *
   * @return string of the bench description
   */
  public abstract String getBenchDescription();

  /**
   * Runs the test locally, in process.
   *
   * @return the task result
   */
  public abstract T runLocal() throws Exception;

  /**
   * Prepares to run the test.
   */
  // TODO(bowen): When the test runs in cluster mode, the prepare step will execute
  //  both in the command side and on each job worker side. We should separate the logic
  //  into two different calls instead of relying on the same prepare().
  public abstract void prepare() throws Exception;

  /**
   * Perform post-run cleanups.
   */
  public void cleanup() throws Exception {}

  protected static void mainInternal(String[] args, Benchmark benchmark) {
    int exitCode = 0;
    try {
      String result = benchmark.run(args);
      System.out.println(result);
    } catch (Exception e) {
      e.printStackTrace();
      exitCode = -1;
    } finally {
      try {
        benchmark.cleanup();
      } catch (Exception e) {
        e.printStackTrace();
        exitCode = -1;
      }
    }
    System.exit(exitCode);
  }

  /**
   * Generate a {@link StressBenchConfig} as the default JobConfig.
   *
   * @param args arguments
   * @return the JobConfig
   * */
  public PlanConfig generateJobConfig(String[] args) {
    // remove the cluster flag and java opts
    List<String> commandArgs = Arrays.stream(args).filter((s) ->
        !BaseParameters.CLUSTER_FLAG.equals(s) && !s.isEmpty())
        .collect(Collectors.toList());

    commandArgs.addAll(mBaseParameters.mJavaOpts.stream().map(String::trim)
        .collect(Collectors.toList()));
    String className = this.getClass().getCanonicalName();
    long startDelay = FormatUtils.parseTimeSize(mBaseParameters.mClusterStartDelay);
    return new StressBenchConfig(className, commandArgs, startDelay, mBaseParameters.mClusterLimit);
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
        System.out.println(getBenchDescription());
        jc.usage();
        System.exit(0);
      }
    } catch (Exception e) {
      LOG.error("Failed to parse command: ", e);
      System.out.println(getBenchDescription());
      jc.usage();
      throw e;
    }

    // prepare the benchmark.
    prepare();

    if (!mBaseParameters.mProfileAgent.isEmpty()) {
      mBaseParameters.mJavaOpts.add("-javaagent:" + mBaseParameters.mProfileAgent
          + "=" + BaseParameters.AGENT_OUTPUT_PATH);
    }

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
      command.addAll(mBaseParameters.mJavaOpts.stream().map(String::trim)
          .collect(Collectors.toList()));

      LOG.info("running command: " + String.join(" ", command));
      return ShellUtils.execCommand(command.toArray(new String[0]));
    }
  }

  /**
   *
   * @param startMs the start time
   * @param endMs the end time
   * @param nameTransformer function which transforms the type and method into a name. If the
   *                        function returns null, then the method is skipped
   * @return a map of names to statistics
   */
  @SuppressFBWarnings(value = "DMI_HARDCODED_ABSOLUTE_FILENAME")
  protected Map<String, MethodStatistics> processMethodProfiles(long startMs, long endMs,
      Function<ProfileInput, String> nameTransformer) throws IOException {
    Map<String, MethodStatistics> nameStatistics = new HashMap<>();

    try (final BufferedReader reader = new BufferedReader(
        new FileReader(BaseParameters.AGENT_OUTPUT_PATH))) {
      String line;

      long bucketSize = (endMs - startMs) / StressConstants.MAX_TIME_COUNT;

      final ObjectMapper objectMapper = new ObjectMapper();
      while ((line = reader.readLine()) != null) {
        final Map<String, Object> lineMap;
        try {
          lineMap = objectMapper.readValue(line, Map.class);
        } catch (JsonParseException | MismatchedInputException e) {
          // skip the last line of a not completed file
          break;
        }

        final String type = (String) lineMap.get("type");
        final String methodName = (String) lineMap.get("methodName");
        final Number timestampNumber = (Number) lineMap.get("timestamp");
        final Number durationNumber = (Number) lineMap.get("duration");
        final Boolean ttfbFlag = (Boolean) lineMap.get("ttfb");

        if (type == null || methodName == null || timestampNumber == null
            || durationNumber == null || ttfbFlag == null) {
          continue;
        }

        final long timestamp = timestampNumber.longValue();
        final long duration = durationNumber.longValue();
        final boolean ttfb = ttfbFlag.booleanValue();

        if (timestamp <= startMs) {
          continue;
        }

        ProfileInput profileInput = new ProfileInput(type, methodName, ttfb);
        final String name = nameTransformer.apply(profileInput);
        if (name == null) {
          continue;
        }

        if (!nameStatistics.containsKey(name)) {
          nameStatistics.put(name, new MethodStatistics());
        }
        final MethodStatistics statistic = nameStatistics.get(name);

        statistic.mTimeNs.recordValue(duration);
        statistic.mNumSuccess += 1;

        int bucket =
            Math.min(statistic.mMaxTimeNs.length - 1, (int) ((timestamp - startMs) / bucketSize));
        statistic.mMaxTimeNs[bucket] = Math.max(statistic.mMaxTimeNs[bucket], duration);
      }
    }
    return nameStatistics;
  }

  protected static final class ProfileInput {
    private final String mType;
    private final String mMethod;
    private final boolean mIsttfb;

    ProfileInput(String type, String method, boolean isttfb) {
      mType = type;
      mMethod = method;
      mIsttfb = isttfb;
    }

    /**
     * @return class type
     */
    public String getType() {
      return mType;
    }

    /**
     * @return method name
     */
    public String getMethod() {
      return mMethod;
    }

    /**
     * @return is time to first byte
     */
    public boolean getIsttfb() {
      return mIsttfb;
    }
  }

  protected static final class MethodStatistics {
    private Histogram mTimeNs;
    private int mNumSuccess;
    private long[] mMaxTimeNs;

    MethodStatistics() {
      mNumSuccess = 0;
      mTimeNs = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
          StressConstants.TIME_HISTOGRAM_PRECISION);
      mMaxTimeNs = new long[StressConstants.MAX_TIME_COUNT];
      Arrays.fill(mMaxTimeNs, -1);
    }

    /**
     * @return the time histogram
     */
    public Histogram getTimeNs() {
      return mTimeNs;
    }

    /**
     * @return number of successes
     */
    public int getNumSuccess() {
      return mNumSuccess;
    }

    /**
     * @return the max time measurement, over the duration of the test
     */
    public long[] getMaxTimeNs() {
      return mMaxTimeNs;
    }
  }
}
