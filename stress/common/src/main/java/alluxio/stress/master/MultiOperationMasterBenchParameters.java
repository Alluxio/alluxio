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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * This holds all the parameters. All fields are public for easier json ser/de without all the
 * getters and setters.
 */
public final class MultiOperationMasterBenchParameters extends MasterBenchBaseParameters {

  public static final String OPERATIONS_OPTION_NAME = "--operations";
  public static final String OPERATIONS_RATIO_OPTION_NAME = "--operations-ratio";
  public static final String BASES_OPTION_NAME = "--bases";
  public static final String FIXED_COUNTS_OPTION_NAME = "--fixed-counts";

  public static final String SKIP_PREPARE_OPTION_NAME = "--skip-prepare";
  public static final String THREADS_RATIO_OPTION_NAME = "--threads-ratio";
  public static final String TARGET_THROUGHPUTS_OPTION_NAME = "--target-throughputs";
  public static final String DURATION_OPTION_NAME = "--duration";

  @Parameter(names = {OPERATIONS_OPTION_NAME},
      description = "A set of operations to test."
          + " Operations are separated by comma (e.g., CreateFile,GetFileStatus) "
          + "and no extra whitespace is allowed.",
      converter = OperationsConverter.class,
      required = true)
  public Operation[] mOperations;
  @Parameter(names = {BASES_OPTION_NAME},
      description = "A set of base directories for operations. "
          + "Directories are separated by comma (e.g., alluxio:///test0,alluxio:///test1) "
          + "and no extra whitespace is allowed.",
      converter = StringArrayConverter.class,
      required = false)
  public String[] mBasePaths = new String[] {
      "alluxio:///stress-master-base-0",
      "alluxio:///stress-master-base-1"
  };
  @Parameter(names = {FIXED_COUNTS_OPTION_NAME},
      description = "A set of fixed-counts for operations. "
          + "Fixed-counts are separated by comma (e.g., 256,256)."
          + "and no extra whitespace is allowed.",
      converter = IntegerArrayConverter.class,
      required = false)
  public int[] mFixedCounts;
  @Parameter(names = {OPERATIONS_RATIO_OPTION_NAME},
      description = "A set of integers specifying the throughput ratio of each operation. "
          + " Numbers are separated by comma (e.g., 3,7) and no extra whitespace is allowed. "
          + " E.g. if you want 30% of your operations to be the first operation and the rest 70% "
          + " to be the second operation. You can specify this param to be (3,7) or (0.3, 0.7) "
          + " Numbers will be normalized so only the ratio matters."
          + " Cannot be set together with " + TARGET_THROUGHPUTS_OPTION_NAME
          + " and " + THREADS_RATIO_OPTION_NAME,
      converter = RatioConverter.class,
      required = false)
  public double[] mOperationsRatio;
  @Parameter(names = {TARGET_THROUGHPUTS_OPTION_NAME},
      description = "A set of target-throughput for operations,"
          + " separated by comma (e.g., 1000,1000). No extra whitespace is allowed."
          + " Cannot be set together with " + TARGET_THROUGHPUTS_OPTION_NAME
          + " and " + THREADS_RATIO_OPTION_NAME,
      converter = IntegerArrayConverter.class,
      required = false)
  public int[] mTargetThroughputs;

  @Parameter(names = {THREADS_RATIO_OPTION_NAME},
      description = "A set of integers specifying "
          + "the ratio of threads each operation is assigned with. "
          + "Number are separated by comma (e.g., 3,7). No extra whitespace is allowed. "
          + "If there are 16 threads and you want 4 threads assigned to the first operation and "
          + "12 for the other, you can set this param to be (4,12) or (1,3)",
      converter = RatioConverter.class,
      required = false)
  public double[] mThreadsRatio;

  @Parameter(names = {SKIP_PREPARE_OPTION_NAME},
      description = "If true, skip the preparation.")
  public boolean mSkipPrepare = false;

  @Parameter(names = {DURATION_OPTION_NAME},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  /**
   * Parses operations param.
   */
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

  /**
   * Parses ratio params.
   */
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

  /**
   * Parses integer array params.
   */
  public static class IntegerArrayConverter implements IStringConverter<int[]> {
    @Override
    public int[] convert(String s) {
      String[] integerStrings = s.split(",");
      int[] integers = new int[integerStrings.length];
      for (int i = 0; i < integerStrings.length; i++) {
        integers[i] = Integer.parseInt(integerStrings[i]);
      }
      return integers;
    }
  }

  /**
   * Parses string array params.
   */
  public static class StringArrayConverter implements IStringConverter<String[]> {
    @Override
    public String[] convert(String s) {
      return s.split(",");
    }
  }
}
