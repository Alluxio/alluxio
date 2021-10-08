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

package alluxio.stress.master;

import alluxio.stress.Parameters;
import alluxio.stress.common.FileSystemParameters;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

/**
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class MasterBenchParameters extends FileSystemParameters {
  /** The stop count value that is invalid. */
  public static final int STOP_COUNT_INVALID = -1;

  @Parameter(names = {"--operation"},
      description = "the operation to perform. Options are [CreateFile, GetBlockLocations, "
          + "GetFileStatus, OpenFile, CreateDir, ListDir, ListDirLocated, RenameFile, DeleteFile]",
      converter = OperationConverter.class,
      required = true)
  public Operation mOperation;

  @Parameter(names = {"--clients"}, description = "the number of fs client instances to use")
  public int mClients = 1;

  @Parameter(names = {"--threads"}, description = "the number of concurrent threads to use")
  public int mThreads = 256;

  @Parameter(names = {"--target-throughput"},
      description = "the target throughput to issue operations. (ops / s)")
  public int mTargetThroughput = 1000;

  @Parameter(names = {"--base"},
      description = "The base directory path URI to perform operations in")
  @Parameters.PathDescription(aliasFieldName = "mBaseAlias")
  public String mBasePath = "alluxio://localhost:19998/stress-master-base";

  @Parameter(names = {"--base-alias"}, description = "The alias for the base path, unused if empty")
  @Parameters.KeylessDescription
  public String mBaseAlias = "";

  @Parameter(names = {"--tag"}, description = "optional human-readable string to identify this run")
  @Parameters.KeylessDescription
  public String mTag = "";

  @Parameter(names = {"--create-file-size"},
      description = "The size of a file for the Create op, allowed to be 0. (0, 1m, 2k, 8k, etc.)")
  public String mCreateFileSize = "0";

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

  @Parameter(names = {"--fixed-count"},
      description = "The number of paths in the fixed portion. Must be greater than 0. The first "
          + "'fixed-count' paths are in the fixed portion of the namespace. This means all tasks "
          + "are guaranteed to have the same number of paths in the fixed portion. This is "
          + "primarily useful for ensuring different tasks/threads perform an identically-sized "
          + "operation. For example, if fixed-count is set to 1000, and CreateFile is run, each "
          + "task will create files with exactly 1000 paths in the fixed directory. A subsequent "
          + "ListDir task will list that directory, knowing every task/thread will always read a "
          + "directory with exactly 1000 paths.")
  public int mFixedCount = 100;

  @DynamicParameter(names = "--conf",
      description = "Any HDFS client configuration key=value. Can repeat to provide multiple "
          + "configuration values.")

  public Map<String, String> mConf = new HashMap<>();

  @Parameter(names = {"--skip-prepare"},
      description = "If true, skip the prepare.")
  public boolean mSkipPrepare = false;

  /**
   * Converts from String to Operation instance.
   */
  public static class OperationConverter implements IStringConverter<Operation> {
    @Override
    public Operation convert(String value) {
      return Operation.fromString(value);
    }
  }
}
