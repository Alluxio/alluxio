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

import static alluxio.stress.master.MultiOperationMasterBenchParameters.BASES_OPTION_NAME;
import static alluxio.stress.master.MultiOperationMasterBenchParameters.OPERATIONS_OPTION_NAME;
import static alluxio.stress.master.MultiOperationMasterBenchParameters.OPERATIONS_RATIO_OPTION_NAME;
import static alluxio.stress.master.MultiOperationMasterBenchParameters.TARGET_THROUGHPUTS_OPTION_NAME;
import static alluxio.stress.master.MultiOperationMasterBenchParameters.THREADS_RATIO_OPTION_NAME;

import alluxio.AlluxioURI;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.stress.StressConstants;
import alluxio.stress.common.FileSystemClientType;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.stress.master.MasterBenchTaskResultStatistics;
import alluxio.stress.master.MultiOperationMasterBenchParameters;
import alluxio.stress.master.MultiOperationMasterBenchTaskResult;
import alluxio.stress.master.Operation;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import org.HdrHistogram.Histogram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * A stress master bench that benchmark a set of combined operations.
 */
// TODO(jiacheng): avoid the implicit casts and @SuppressFBWarnings
public class MultiOperationStressMasterBench
    extends StressMasterBenchBase<MultiOperationMasterBenchTaskResult,
    MultiOperationMasterBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiOperationStressMasterBench.class);

  /**
   * Creates instance.
   */
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public MultiOperationStressMasterBench() {
    super(new MultiOperationMasterBenchParameters());
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new MultiOperationStressMasterBench());
  }

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A benchmarking tool to measure the master performance of Alluxio",
        "This benchmark is a variant of StressMasterBench, where a user can specify multiple ",
        "operations to run concurrently with some constraints to limit the throughput of ",
        "each operations.",
        "",
        "Example:",
        "# this would continuously run `GetFileStatus` and CreateFile "
        + "for 30s and record the throughput after 5s warmup. "
        + "The CreateFile throughput will be 4x more than GetFileStatus.",
        "$ bin/alluxio runClass alluxio.stress.cli.MultiOperationStressMasterBench \\",
        "--operations GetFileStatus,CreateFile --operations-ratio 1,4  --fixed-counts 100,1 \\",
        "--warmup 1s  --duration 10s -client-type AlluxioNative --threads 16 ",
        "",
        "Use the following command to prepare the test directory to list with before running test:",
        "$ bin/alluxio runClass alluxio.stress.cli.StressMasterBench \\",
        "--base alluxio:///stress-master-base-0 --fixed-count 100 --stop-count 100 --warmup 5s \\",
        "--operation CreateFile --duration 30s --client-type AlluxioNative"
    ));
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public void validateParams() throws Exception {
    int numOperations = mParameters.mOperations.length;
    if (numOperations < 2) {
      throw new InvalidArgumentException(String.format(
          "%s operations must contain at least two operations.", OPERATIONS_OPTION_NAME));
    }

    if (mParameters.mBasePaths.length != mParameters.mOperations.length) {
      throw new InvalidArgumentException(
          String.format("%s must contain the same number of params as %s .",
              BASES_OPTION_NAME, OPERATIONS_OPTION_NAME));
    }

    InvalidArgumentException duplicatedModeSpecifiedException = new InvalidArgumentException(
        String.format("Exact one of the following params %s, %s, %s must be specified",
            TARGET_THROUGHPUTS_OPTION_NAME, THREADS_RATIO_OPTION_NAME,
            OPERATIONS_RATIO_OPTION_NAME));

    boolean modeSpecified = false;
    if (mParameters.mTargetThroughputs != null) {
      if (mParameters.mTargetThroughputs.length != numOperations) {
        throw new InvalidArgumentException(
            String.format("If specified, %s must contain the same number of args as %s.",
                TARGET_THROUGHPUTS_OPTION_NAME, OPERATIONS_OPTION_NAME));
      }
      modeSpecified = true;
    }

    if (mParameters.mThreadsRatio != null) {
      if (mParameters.mThreadsRatio.length != numOperations) {
        throw new InvalidArgumentException(
            String.format("If specified, %s must contain the same number of args as %s.",
                THREADS_RATIO_OPTION_NAME, OPERATIONS_OPTION_NAME));
      }
      if (modeSpecified) {
        throw duplicatedModeSpecifiedException;
      }
      modeSpecified = true;
    }

    if (mParameters.mOperationsRatio != null) {
      if (mParameters.mOperationsRatio.length != numOperations) {
        throw new InvalidArgumentException(
            String.format("If specified, %s must contain the same number of args as %s.",
                OPERATIONS_RATIO_OPTION_NAME, OPERATIONS_OPTION_NAME));
      }
      if (modeSpecified) {
        throw duplicatedModeSpecifiedException;
      }
      modeSpecified = true;
    }

    if (!modeSpecified) {
      throw duplicatedModeSpecifiedException;
    }
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public void prepare() throws Exception {
    if (!mBaseParameters.mDistributed) {
      // set hdfs conf for preparation client
      Configuration hdfsConf = new Configuration();
      // force delete, create dirs through to UFS
      hdfsConf.set(PropertyKey.Name.USER_FILE_DELETE_UNCHECKED, "true");
      hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, mParameters.mWriteType);
      // more threads for parallel deletes for cleanup
      hdfsConf.set(PropertyKey.Name.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX, "256");
      FileSystem prepareFs = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);

      for (int i = 0; i < mParameters.mOperations.length; i++) {
        Operation op = mParameters.mOperations[i];
        // initialize the base, for only the non-distributed task (the cluster launching task)
        Path path = new Path(mParameters.mBasePaths[i]);
        // the base path depends on the operation
        Path basePath;
        if (op == Operation.CREATE_DIR) {
          basePath = new Path(path, "dirs");
        } else {
          basePath = new Path(path, "files");
        }
        if (!mParameters.mSkipPrepare) {
          if (op == Operation.CREATE_FILE
              || op == Operation.CREATE_DIR) {
            LOG.info("Cleaning base path: {}", basePath);
            long start = CommonUtils.getCurrentMs();
            deletePaths(prepareFs, basePath);
            long end = CommonUtils.getCurrentMs();
            LOG.info("Cleanup took: {} s", (end - start) / 1000.0);
            prepareFs.mkdirs(basePath);
          } else {
            // these are read operations. the directory must exist
            if (!prepareFs.exists(basePath)) {
              throw new IllegalStateException(String
                  .format("base path (%s) must exist for operation (%s)", basePath,
                      op));
            }
          }
        }
        if (!prepareFs.isDirectory(basePath)) {
          throw new IllegalStateException(String
              .format("base path (%s) must be a directory for operation (%s)", basePath,
                  op));
        }
      }
    }

    if (mParameters.mClientType == FileSystemClientType.ALLUXIO_HDFS) {
      throw new RuntimeException(
          "ALLUXIO HDFS Compatible API is not support for multi operations stress master bench");
    }

    // set hdfs conf for all test clients
    LOG.info("Using ALLUXIO Native API to perform the test.");
    InstancedConfiguration alluxioProperties = alluxio.conf.Configuration.copyGlobal();
    alluxioProperties.set(
        PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, mParameters.mWriteType, Source.RUNTIME);
    mCachedNativeFs = new alluxio.client.file.FileSystem[mParameters.mClients];
    for (int i = 0; i < mCachedNativeFs.length; i++) {
      mCachedNativeFs[i] = alluxio.client.file.FileSystem.Factory
          .create(alluxioProperties);
    }

    for (int i = 0; i < mParameters.mOperations.length; ++i) {
      if (mParameters.mOperations[i] == Operation.CRURD
          || mParameters.mOperations[i] == Operation.CREATE_DELETE_FILE) {
        AlluxioURI uri = new AlluxioURI(
            PathUtils.concatPath(mParameters.mBasePaths[i], mFilesDir, mBaseParameters.mId));
        if (mCachedNativeFs[0].exists(uri)) {
          mCachedNativeFs[0].delete(uri, DeletePOptions.newBuilder().setRecursive(true).build());
        }
        mCachedNativeFs[0].createDirectory(uri, CreateDirectoryPOptions.getDefaultInstance());
      }
    }
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  protected BenchThread getBenchThread(BenchContext context, int index) {
    if (mParameters.mClientType == FileSystemClientType.ALLUXIO_NATIVE) {
      return new AlluxioNativeBenchThread(context,
          mCachedNativeFs[index % mCachedNativeFs.length], index);
    }
    throw new RuntimeException(
        "ALLUXIO HDFS and POSIX API is not support for multi operations stress master bench");
  }

  private RateLimiter createUnlimitedRateLimiter() {
    return RateLimiter.create(Double.MAX_VALUE);
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  protected StressMasterBenchBase<
      MultiOperationMasterBenchTaskResult, MultiOperationMasterBenchParameters>.BenchContext
      getContext() throws IOException, AlluxioException {
    final BenchContext benchContext;
    if (mParameters.mTargetThroughputs == null) {
      RateLimiter[] rateLimiters = new RateLimiter[mParameters.mOperations.length];
      for (int i = 0; i < mParameters.mTargetThroughputs.length; i++) {
        rateLimiters[i] = createUnlimitedRateLimiter();
      }
      benchContext = new BenchContext(
          createUnlimitedRateLimiter(), rateLimiters,
          mParameters.mOperations, mParameters.mBasePaths, mParameters.mDuration);
    } else {
      int sum = 0;
      RateLimiter[] rateLimiters = new RateLimiter[mParameters.mOperations.length];
      for (int i = 0; i < mParameters.mTargetThroughputs.length; i++) {
        rateLimiters[i] = RateLimiter.create(mParameters.mTargetThroughputs[i]);
        sum += mParameters.mTargetThroughputs[i];
      }
      benchContext = new BenchContext(
          RateLimiter.create(sum), rateLimiters, mParameters.mOperations, mParameters.mBasePaths,
          mParameters.mDuration);
    }
    return benchContext;
  }

  protected abstract class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final Histogram[] mResponseTimeNs;
    protected final Path[] mBasePaths;
    protected final Path[] mFixedBasePaths;
    private final MultiOperationMasterBenchTaskResult mResult;
    private final int mThreadIndex;
    private final Random mRandom = new Random();

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private BenchThread(
        BenchContext context, int threadIndex) {
      mContext = context;
      mResponseTimeNs = new Histogram[mParameters.mOperations.length];
      mBasePaths = mContext.getBasePaths();
      mFixedBasePaths = mContext.getFixedPaths();

      mResult = new MultiOperationMasterBenchTaskResult(mParameters.mOperations);
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        mResponseTimeNs[i] = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
            StressConstants.TIME_HISTOGRAM_PRECISION);
      }
      mThreadIndex = threadIndex;
    }

    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    public Void call() {
      try {
        if (mParameters.mOperationsRatio != null) {
          runOperationsRatioBased();
        } else if (mParameters.mThreadsRatio != null) {
          runThreadsRatioBased();
        } else {
          runInternalTargetThroughputBased();
        }
      } catch (Exception e) {
        e.printStackTrace();
        LOG.warn("Exception during bench thread runInternal", e);
        mResult.addErrorMessage(e.getMessage());
      }

      // Update local thread result
      mResult.setEndMs(CommonUtils.getCurrentMs());
      List<MasterBenchTaskResultStatistics> statistics = mResult.getAllStatistics();
      for (int i = 0; i < statistics.size(); i++) {
        statistics.get(i).encodeResponseTimeNsRaw(mResponseTimeNs[i]);
      }
      mResult.setParameters(mParameters);
      mResult.setBaseParameters(mBaseParameters);

      // merge local thread result with full result
      mContext.mergeThreadResult(mResult);

      return null;
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runInternal(Function<Integer, Integer> operationIndexSelector) throws Exception {
      // When to start recording measurements
      long recordMs = mContext.getStartMs() + FormatUtils.parseTimeSize(mParameters.mWarmup);
      mResult.setRecordStartMs(recordMs);

      boolean useStopCount = mParameters.mStopCount != MasterBenchParameters.STOP_COUNT_INVALID;

      long bucketSize = (mContext.getEndMs() - recordMs) / StressConstants.MAX_TIME_COUNT;

      long waitMs = mContext.getStartMs() - CommonUtils.getCurrentMs();
      if (waitMs < 0) {
        throw new IllegalStateException(String.format(
            "Thread missed barrier. Increase the start delay. start: %d current: %d",
            mContext.getStartMs(), CommonUtils.getCurrentMs()));
      }
      CommonUtils.sleepMs(waitMs);

      long totalCounter;
      int operationIndex = 0;
      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        if (!useStopCount && CommonUtils.getCurrentMs() >= mContext.getEndMs()) {
          break;
        }
        totalCounter = mContext.getTotalCounter().incrementAndGet() - 1;
        if (useStopCount && totalCounter >= mParameters.mStopCount) {
          break;
        }

        long operationCount;
        operationIndex = operationIndexSelector.apply(operationIndex);
        operationCount = mContext.getOperationCounter(operationIndex).incrementAndGet() - 1;
        long startNs = System.nanoTime();
        applyOperation(operationIndex, operationCount);
        long endNs = System.nanoTime();

        long currentMs = CommonUtils.getCurrentMs();
        // Start recording after the warmup
        if (currentMs > recordMs) {
          mResult.incrementNumSuccess(operationIndex, 1);

          // record response times
          long responseTimeNs = endNs - startNs;
          mResponseTimeNs[operationIndex].recordValue(responseTimeNs);

          // track max response time
          long[] maxResponseTimeNs = mResult.getStatistics(operationIndex).mMaxResponseTimeNs;
          int bucket =
              Math.min(maxResponseTimeNs.length - 1, (int) ((currentMs - recordMs) / bucketSize));
          if (responseTimeNs > maxResponseTimeNs[bucket]) {
            maxResponseTimeNs[bucket] = responseTimeNs;
          }
        }
      }
    }

    /**
     * Gets the ratio array and normalizes it into probability distribution
     * e.g. [4,6,10] will be normalized into [0.2, 0.5, 1.0]
     * @return the probability distribution array
     */
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private double[] getProbabilityDistribution(double[] ratios) {
      double ratioSum = 0;
      double last = 0;
      double[] probabilities = new double[ratios.length];
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        ratioSum += mParameters.mOperationsRatio[i];
      }
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        probabilities[i] = last + mParameters.mOperationsRatio[i] / ratioSum;
        last = probabilities[i];
      }
      return probabilities;
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runOperationsRatioBased() throws Exception {
      double[] probabilities = getProbabilityDistribution(mParameters.mOperationsRatio);
      //Each run randomly picks an operation based on the probability array
      runInternal(lastOperationIndex -> {
        double r = mRandom.nextDouble();
        int operationIndex = 0;
        for (int i = 0; i < mParameters.mOperations.length; i++) {
          if (probabilities[i] >= r) {
            operationIndex = i;
            break;
          }
        }
        mContext.getRateLimiter(0).acquire();
        return operationIndex;
      });
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runThreadsRatioBased() throws Exception {
      double[] probabilities = getProbabilityDistribution(mParameters.mThreadsRatio);
      int operationIndex = 0;
      //Each thread randomly picks an operation to do based on the probability array
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        if (probabilities[i] >= (double) mThreadIndex / mParameters.mThreads) {
          operationIndex = i;
          break;
        }
      }
      int finalOperationIndex = operationIndex;
      runInternal(lastOperationIndex -> {
        mContext.getRateLimiter(finalOperationIndex).acquire();
        return finalOperationIndex;
      });
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runInternalTargetThroughputBased() throws Exception {
      RateLimiter[] rls = mContext.getRateLimiters();
      // Each run picks the next available operation with the following 2 constraints:
      // 1. the overall throughput should not exceed the user specified limit
      // 2. the throughput of the operation to execute should not exceed the user specified limit,
      //    otherwise, try the next eligible one.
      runInternal((lastOperationIndex) -> {
        int operationIndex = (lastOperationIndex + 1) % mParameters.mOperations.length;
        mContext.getGrandRateLimiter().acquire();
        while (!rls[operationIndex].tryAcquire()) {
          operationIndex++;
          operationIndex %= mParameters.mOperations.length;
        }
        return operationIndex;
      });
    }

    protected abstract void applyOperation(int operationIndex, long operationCounter)
        throws IOException, AlluxioException;
  }

  private final class AlluxioNativeBenchThread extends BenchThread {
    private final alluxio.client.file.FileSystem mFs;

    private AlluxioNativeBenchThread(
        BenchContext context,
        alluxio.client.file.FileSystem fs, int threadIndex) {
      super(context, threadIndex);
      mFs = fs;
    }

    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    protected void applyOperation(int operationIndex, long operationCounter)
        throws IOException, AlluxioException {
      MultiOperationStressMasterBench.this.applyNativeOperation(
          mFs, mParameters.mOperations[operationIndex], operationCounter,
          mBasePaths[operationIndex], mFixedBasePaths[operationIndex],
          mParameters.mFixedCounts[operationIndex]);
    }
  }
}
