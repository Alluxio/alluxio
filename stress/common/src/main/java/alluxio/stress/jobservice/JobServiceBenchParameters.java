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

package alluxio.stress.jobservice;

import alluxio.stress.Parameters;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class JobServiceBenchParameters extends Parameters {
  /** The stop count value that is invalid. */
  public static final int STOP_COUNT_INVALID = -1;

  @Parameter(names = {"--operation"},
      description = "the operation to perform. Options are [CreateFile, GetBlockLocations, "
          + "GetFileStatus, OpenFile, CreateDir, ListDir, ListDirLocated, RenameFile, DeleteFile]",
      converter = OperationConverter.class,
      required = true)
  public JobServiceBenchOperation mOperation;


  @Parameter(names = {"--clients"}, description = "the number of fs client instances to use")
  public int mClients = 1;

  @Parameter(names = {"--requests"}, description = "the number of concurrent requests")
  public int mNumRequests = 256;

  @Parameter(names = {"--files-per-request"},
      description = "the number of files to be loaded in each request.")
  public int mNumFilesPerRequest = 1000;

  @Parameter(names = {"--target-scalability"},
      description = "the target throughput to issue operations. (ops / s)")
  public int mTargetScalability = 1000;

  @Parameter(names = {"--base"},
      description = "The base directory path URI to perform operations in")
  @PathDescription(aliasFieldName = "mBaseAlias")
  public String mBasePath = "alluxio://localhost:19998/stress-job-service-base";

  @Parameter(names = {"--base-alias"}, description = "The alias for the base path, unused if empty")
  @KeylessDescription
  public String mBaseAlias = "";

  @Parameter(names = {"--tag"}, description = "optional human-readable string to identify this run")
  @KeylessDescription
  public String mTag = "";

  @Parameter(names = {"--file-size"},
      description = "The size of a file for the Create op, allowed to be 0. (0, 1m, 2k, 8k, etc.)")
  public int mFileSize = 128;

  @Parameter(names = {"--duration"},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {"--warmup"},
      description = "The length of time to warmup before recording measurements. (1m, 10m, 60s, "
          + "10000ms, etc.)")
  public String mWarmup = "30s";

  @Parameter(names = {"--stop-count"},
      description = "The benchmark will stop after this number of paths. If -1, it is not used and "
          + "the benchmark will stop after the duration. If this is used, duration will be "
          + "ignored. This is typically used for creating files in preparation for another "
          + "benchmark, since the results may not be reliable with a non-duration-based "
          + "termination condition.")
  public int mStopCount = STOP_COUNT_INVALID;

  @DynamicParameter(names = "--conf",
      description = "Any HDFS client configuration key=value. Can repeat to provide multiple "
          + "configuration values.")

  public Map<String, String> mConf = new HashMap<>();

  /**
   * Converts from String to Operation instance.
   */
  public static class OperationConverter implements IStringConverter<JobServiceBenchOperation> {
    @Override
    public JobServiceBenchOperation convert(String value) {
      return JobServiceBenchOperation.fromString(value);
    }
  }
}
