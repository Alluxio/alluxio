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

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

/**
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class MasterBenchParameters extends MasterBenchBaseParameters {
  public static final String OPERATION_OPTION_NAME = "--operation";
  public static final String TARGET_THROUGHPUT_OPTION_NAME = "--target-throughput";
  public static final String BASE_ALIAS_OPTION_NAME = "--base-alias";
  public static final String TAG_OPTION_NAME = "--tag";
  public static final String DURATION_OPTION_NAME = "--duration";
  public static final String FIXED_COUNT_OPTION_NAME = "--fixed-count";
  public static final String CONF_OPTION_NAME = "--conf";
  public static final String SKIP_PREPARE_OPTION_NAME = "--skip-prepare";

  @Parameter(names = {OPERATION_OPTION_NAME},
      description = "the operation to perform. Options are [CreateFile, GetBlockLocations, "
          + "GetFileStatus, OpenFile, CreateDir, ListDir, ListDirLocated, RenameFile, DeleteFile]",
      converter = OperationConverter.class,
      required = true)
  public Operation mOperation;

  @Parameter(names = {TARGET_THROUGHPUT_OPTION_NAME},
      description = "the target throughput to issue operations. (ops / s)")
  public int mTargetThroughput = 1000;

  @Parameter(names = {BASE_ALIAS_OPTION_NAME},
      description = "The alias for the base path, unused if empty")
  @Parameters.KeylessDescription
  public String mBaseAlias = "";

  @Parameter(names = {TAG_OPTION_NAME},
      description = "optional human-readable string to identify this run")
  @Parameters.KeylessDescription
  public String mTag = "";

  @Parameter(names = {DURATION_OPTION_NAME},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {FIXED_COUNT_OPTION_NAME},
      description = "The number of paths in the fixed portion. Must be greater than 0. The first "
          + "'fixed-count' paths are in the fixed portion of the namespace. This means all tasks "
          + "are guaranteed to have the same number of paths in the fixed portion. This is "
          + "primarily useful for ensuring different tasks/threads perform an identically-sized "
          + "operation. For example, if fixed-count is set to 1000, and CreateFile is run, each "
          + "task will create files with exactly 1000 paths in the fixed directory. A subsequent "
          + "ListDir task will list that directory, knowing every task/thread will always read a "
          + "directory with exactly 1000 paths. A task such as OpenFile will repeatedly read the "
          + "1000 files so that the task will not end before the desired duration time.")
  public int mFixedCount = 100;

  @DynamicParameter(names = CONF_OPTION_NAME,
      description = "Any HDFS client configuration key=value. Can repeat to provide multiple "
          + "configuration values.")

  public Map<String, String> mConf = new HashMap<>();

  @Parameter(names = {SKIP_PREPARE_OPTION_NAME},
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
