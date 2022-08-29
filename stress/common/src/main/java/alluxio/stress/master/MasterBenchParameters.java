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
@SuppressWarnings("checkstyle:RegexpSingleline")
public final class MasterBenchParameters extends MasterBenchBaseParameters {
  public static final String OPERATION_OPTION_NAME = "--operation";
  public static final String OPERATIONS_OPTION_NAME = "--operations";
  public static final String OPERATIONS_RATIO_OPTION_NAME = "--operations-ratio";
  public static final String BASES_OPTION_NAME = "--bases";
  public static final String FIXED_COUNT_OPTION_NAME = "--fixed-count";
  public static final String FIXED_COUNTS_OPTION_NAME = "--fixed-counts";
  public static final String TARGET_THROUGHPUT_OPTION_NAME = "--target-throughput";
  public static final String COUNTERS_OFFSET_OPTION_NAME = "--counters-offset";
  public static final String SINGLE_DIR_OPTION_NAME = "--single-dir";
  public static final String THREADS_RATIO_OPTION_NAME = "--threads-ratio";
  public static final String TARGET_THROUGHPUTS_OPTION_NAME = "--target-throughputs";
  public static final String BASE_ALIAS_OPTION_NAME = "--base-alias";
  public static final String TAG_OPTION_NAME = "--tag";
  public static final String DURATION_OPTION_NAME = "--duration";
  public static final String CONF_OPTION_NAME = "--conf";
  public static final String SKIP_PREPARE_OPTION_NAME = "--skip-prepare";

  @Parameter(names = {OPERATION_OPTION_NAME},
      description = "the operation to perform. Options are [CreateFile, GetBlockLocations, "
          + "GetFileStatus, OpenFile, CreateDir, ListDir, ListDirLocated, RenameFile, DeleteFile]",
      converter = OperationConverter.class,
      required = true)
  public Operation mOperation;

  @Parameter(names = {OPERATIONS_OPTION_NAME},
      description = "a set of operations to perform."
          + " Operations are separated by comma(e.g., CreateFile,GetFileStatus)",
      converter = OperationsConverter.class,
      required = true)
  public Operation[] mOperations;
  @Parameter(names = {BASES_OPTION_NAME},
      description = "a set of base directories for operations. "
          + "Directories are separated by comma(e.g., alluxio:///test0,alluxio:///test1)",
      converter = BasePathsConverter.class,
      required = true)
  public String[] mBasePaths;
  @Parameter(names = {FIXED_COUNTS_OPTION_NAME},
      description = "a set of fixed-counts for operations. "
          + "Fixed-counts are separated by comma(e.g., 256,256)",
      converter = FixCountsConverter.class,
      required = true)
  public int[] mFixedCounts;
  @Parameter(names = {OPERATIONS_RATIO_OPTION_NAME},
      description = "a set of real numbers specifying the ratio of each operation "
          + "to the total request. Numbers are separated by comma(e.g., 3,7). "
          + "Each number will be normalized, so the sum of all numbers does not have to be 1.0",
      converter = RatioConverter.class,
      required = false)
  public double[] mOperationsRatio ;
  @Parameter(names = {TARGET_THROUGHPUT_OPTION_NAME},
      description = "the target throughput for all operations. (ops / s)")
  public int mTargetThroughput = 1000;

  @Parameter(names = {TARGET_THROUGHPUTS_OPTION_NAME},
      description = "a set of target-throughput for operations separated by comma(e.g., 1000,1000)",
      converter = TargetThroughputsConverter.class,
      required = false)
  public int[] mTargetThroughputs;

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
      description = "If true, skip the prepare for all operations.")
  public boolean mSkipPrepare = false;

  @Parameter(names = {SINGLE_DIR_OPTION_NAME},
      description = "If true, all workers will operate on the same directory")
  public boolean mSingleDir = false;

  @Parameter(names = {COUNTERS_OFFSET_OPTION_NAME},
      description = "a set of integers used to initialize the counters for each operation. "
          + "Integers are separated by comma(e.g., 0,200). "
          + "They are used to avoid filename conflicts. ",
      converter = CountersOffsetConverter.class,
      required = true)
  public long[] mCountersOffset;

  @Parameter(names = {THREADS_RATIO_OPTION_NAME},
      description = "a set of real number specifying the ratio of threads each operation has. "
          + "Number are separated by comma(e.g., 3,7). "
          + "Each number will be normalized, so the sum of all numbers does not have to be 1.0. "
          + "When you execute a mixed workload "
          + "which contains a heavy operation such performing ListStatus operation on "
          + "a folder with 10K files, other operations will be blocked it. "
          + "Assigning a fixed ratio of threads to each operation will alleviate this situation.:)",
      converter = RatioConverter.class,
      required = false)
  public double[] mThreadsRatio;

  /**
   * Converts from String to Operation instance.
   */
  public static class OperationConverter implements IStringConverter<Operation> {
    @Override
    public Operation convert(String value) {
      return Operation.fromString(value);
    }
  }

  public static class OperationsConverter implements IStringConverter<Operation[]> {
    @Override
    public Operation[] convert(String s) {
      OperationConverter operationConverter = new OperationConverter();
      String[] operationsString = s.split(",");
      Operation[] operations = new Operation[operationsString.length];
      for (int i = 0; i < operationsString.length; i++) {
        operations[i] = operationConverter.convert(operationsString[i]);
      }
      return operations;
    }
  }

  public static class RatioConverter implements IStringConverter<double[]>{
    @Override
    public double[] convert(String s) {
      String[] ratiosString = s.split(",");
      double[] ratios = new double[ratiosString.length];
      for (int i = 0; i < ratiosString.length; i++) {
        ratios[i] = Double.parseDouble(ratiosString[i]);
      }
      return ratios;
    }
  }
  public static class FixCountsConverter implements IStringConverter<int[]>{
    @Override
    public int[] convert(String s) {
      String[] fixCountsString = s.split(",");
      int[] fixCounts = new int[fixCountsString.length];
      for (int i = 0; i < fixCountsString.length; i++) {
        fixCounts[i] = Integer.parseInt(fixCountsString[i]);
      }
      return fixCounts;
    }
  }
  public static class CountersOffsetConverter implements IStringConverter<long[]>{
    @Override
    public long[] convert(String s) {
      String[] countersOffsetString = s.split(",");
      long[] countersOffset = new long[countersOffsetString.length];
      for (int i = 0; i < countersOffsetString.length; i++) {
        countersOffset[i] = Integer.parseInt(countersOffsetString[i]);
      }
      return countersOffset;
    }
  }
  public static class TargetThroughputsConverter implements IStringConverter<int[]>{
    @Override
    public int[] convert(String s) {
      String[] throughputsString = s.split(",");
      int[] targetThroughputs = new int[throughputsString.length];
      for (int i = 0; i < throughputsString.length; i++) {
        targetThroughputs[i] = Integer.parseInt(throughputsString[i]);
      }
      return targetThroughputs;
    }
  }

  public static class BasePathsConverter implements IStringConverter<String[]>{
    @Override
    public String[] convert(String s) {
      String[] basePaths = s.split(",");
      return basePaths;
    }
  }
}
