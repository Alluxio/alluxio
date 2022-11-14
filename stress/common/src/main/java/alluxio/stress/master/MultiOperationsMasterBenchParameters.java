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

import static alluxio.stress.master.MasterBenchParameters.TARGET_THROUGHPUT_OPTION_NAME;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class MultiOperationsMasterBenchParameters extends MasterBenchBaseParameters {
  public static final String OPERATIONS_OPTION_NAME = "--operations";
  public static final String OPERATIONS_RATIO_OPTION_NAME = "--operations-ratio";
  public static final String BASES_OPTION_NAME = "--bases";
  public static final String FIXED_COUNTS_OPTION_NAME = "--fixed-counts";

  public static final String SKIP_PREPARE_OPTION_NAME = "--skip-prepare";

  public static final String COUNTERS_OFFSET_OPTION_NAME = "--counters-offset";
  public static final String SINGLE_DIR_OPTION_NAME = "--single-dir";
  public static final String THREADS_RATIO_OPTION_NAME = "--threads-ratio";
  public static final String TARGET_THROUGHPUTS_OPTION_NAME = "--target-throughputs";

  public static final String DURATION_OPTION_NAME = "--duration";

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
  public double[] mOperationsRatio;
  @Parameter(names = {TARGET_THROUGHPUTS_OPTION_NAME},
      description = "a set of target-throughput for operations separated by comma(e.g., 1000,1000)",
      converter = TargetThroughputsConverter.class,
      required = false)
  public int[] mTargetThroughputs;

  @Parameter(names = {TARGET_THROUGHPUT_OPTION_NAME},
      description = "the target throughput to issue operations. (ops / s)")
  public int mTargetThroughput = 1000;

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

  @Parameter(names = {SKIP_PREPARE_OPTION_NAME},
      description = "If true, skip the prepare.")
  public boolean mSkipPrepare = false;

  @Parameter(names = {DURATION_OPTION_NAME},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  public static class OperationsConverter implements IStringConverter<Operation[]> {
    @Override
    public Operation[] convert(String s) {
      MasterBenchParameters.OperationConverter operationConverter =
          new MasterBenchParameters.OperationConverter();
      String[] operationsString = s.split(",");
      Operation[] operations = new Operation[operationsString.length];
      for (int i = 0; i < operationsString.length; i++) {
        operations[i] = operationConverter.convert(operationsString[i]);
      }
      return operations;
    }
  }

  public static class RatioConverter implements IStringConverter<double[]> {
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

  public static class FixCountsConverter implements IStringConverter<int[]> {
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

  public static class CountersOffsetConverter implements IStringConverter<long[]> {
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

  public static class TargetThroughputsConverter implements IStringConverter<int[]> {
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

  public static class BasePathsConverter implements IStringConverter<String[]> {
    @Override
    public String[] convert(String s) {
      String[] basePaths = s.split(",");
      return basePaths;
    }
  }
}
