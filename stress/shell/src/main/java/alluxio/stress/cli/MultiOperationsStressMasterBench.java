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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.stress.StressConstants;
import alluxio.stress.common.FileSystemClientType;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.stress.master.MasterBenchTaskResultStatistics;
import alluxio.stress.master.MultiOperationsMasterBenchParameters;
import alluxio.stress.master.MultiOperationsMasterBenchTaskResult;
import alluxio.stress.master.Operation;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

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
 * Single node stress test.
 */
public class MultiOperationsStressMasterBench
    extends StressMasterBenchBase<
    MultiOperationsMasterBenchTaskResult, MultiOperationsMasterBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(StressMasterBench.class);

  /**
   * Creates instance.
   */
  public MultiOperationsStressMasterBench() {
    super(new MultiOperationsMasterBenchParameters());
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new MultiOperationsStressMasterBench());
  }

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "This is the description ",
        "MaxThroughput is the recommended way to run the Master Stress Bench.",
        "",
        "Example:",
        "# this would continuously run `ListDir` opeartion for 30s and record the throughput after "
            + "5s warmup.",
        "$ bin/alluxio runClass alluxio.stress.cli.StressMasterBench --operation ListDir \\",
        "--warmup 5s --duration 30s --cluster",
        ""
    ));
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
  }

  @Override
  protected BenchContext<MultiOperationsMasterBenchTaskResult> getContext() {
    final RateLimiter[] rateLimits;
    if (mParameters.mTargetThroughputs != null) {
      int sum = 0;
      rateLimits = new RateLimiter[mParameters.mTargetThroughputs.length + 1];
      for (int i = 0; i < mParameters.mTargetThroughputs.length; i++) {
        rateLimits[i] = RateLimiter.create(mParameters.mTargetThroughputs[i]);
        sum += mParameters.mTargetThroughputs[i];
      }
      rateLimits[rateLimits.length - 1] = RateLimiter.create(sum);
    } else {
      rateLimits = new RateLimiter[] {RateLimiter.create(mParameters.mTargetThroughput)};
    }

    return new BenchContext<MultiOperationsMasterBenchTaskResult>(
        rateLimits, mParameters.mOperations, mParameters.mBasePaths,
        mParameters.mCountersOffset);
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  protected BenchThread getBenchThread(
      BenchContext<MultiOperationsMasterBenchTaskResult> context, int index) {
    switch (mParameters.mClientType) {
      case ALLUXIO_HDFS:
        throw new RuntimeException(
            "ALLUXIO HDFS API is not support for multi operations stress master bench");
      case ALLUXIO_POSIX:
        throw new RuntimeException(
            "ALLUXIO POSIX API is not support for multi operations stress master bench");
      default:
        return new AlluxioNativeBenchThread(context,
            mCachedNativeFs[index % mCachedNativeFs.length], index);
    }
  }

  protected abstract class BenchThread implements Callable<Void> {
    private final BenchContext<MultiOperationsMasterBenchTaskResult> mContext;
    private final Histogram[] mResponseTimeNs;
    protected final Path[] mBasePaths;
    private final MultiOperationsMasterBenchTaskResult mResult;
    private final ThreadLocal<Integer> mThreadIndex = new ThreadLocal<>();
    private final Random mRandom = new Random();

    private BenchThread(
        BenchContext<MultiOperationsMasterBenchTaskResult> context, int threadIndex) {
      mContext = context;
      mResponseTimeNs = new Histogram[mParameters.mOperations.length];
      mBasePaths = mContext.getBasePaths();
      mResult = new MultiOperationsMasterBenchTaskResult(mParameters.mOperations.length);
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        mResponseTimeNs[i] = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
            StressConstants.TIME_HISTOGRAM_PRECISION);
      }
      mThreadIndex.set(threadIndex);
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
        totalCounter = mContext.getTotalCounter().getAndIncrement();
        if (useStopCount && totalCounter >= mParameters.mStopCount) {
          break;
        }

        long operationCount;
        operationCount = mContext.getOperationCounter(operationIndex).getAndIncrement();
        operationIndex = operationIndexSelector.apply(operationIndex);
        long startNs = System.nanoTime();
        applyOperation(operationIndex, operationCount);
        long endNs = System.nanoTime();

        long currentMs = CommonUtils.getCurrentMs();
        // Start recording after the warmup
        if (currentMs > recordMs) {
          mResult.incrementNumSuccess(1, operationIndex);

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

    //Each thread will randomly pick an operation to do by operations ratio.
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runOperationsRatioBased() throws Exception {
      double ratioSum = 0;
      double last = 0;
      double[] ratioArray = new double[mParameters.mOperations.length];
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        ratioSum += mParameters.mOperationsRatio[i];
      }
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        ratioArray[i] = last + mParameters.mOperationsRatio[i] / ratioSum;
        last = ratioArray[i];
      }
      runInternal(lastOperationIndex -> {
        double r = mRandom.nextDouble();
        int operationIndex = 0;
        for (int i = 0; i < mParameters.mOperations.length; i++) {
          if (ratioArray[i] >= r) {
            operationIndex = i;
            break;
          }
        }
        mContext.getRateLimiters()[0].acquire();
        return operationIndex;
      });
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runThreadsRatioBased() throws Exception {
      double ratioSum = 0;
      double last = 0;
      double[] ratioArray = new double[mParameters.mOperations.length];
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        ratioSum += mParameters.mThreadsRatio[i];
      }
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        ratioArray[i] = last + (mParameters.mThreadsRatio[i] / ratioSum) * mParameters.mThreads;
        last = ratioArray[i];
      }
      int operationIndex = 0;
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        if (ratioArray[i] >= mThreadIndex.get()) {
          operationIndex = i;
          break;
        }
      }
      int finalOperationIndex = operationIndex;
      runInternal(lastOperationIndex -> {
        mContext.getRateLimiters()[finalOperationIndex].acquire();
        return finalOperationIndex;
      });
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runInternalTargetThroughputBased() throws Exception {
      RateLimiter[] rls = mContext.getRateLimiters();
      RateLimiter controlRls = rls[rls.length - 1];
      runInternal((lastOperationIndex) -> {
        int operationIndex = lastOperationIndex + 1;
        controlRls.acquire();
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
        BenchContext<MultiOperationsMasterBenchTaskResult> context,
        alluxio.client.file.FileSystem fs, int index) {
      super(context, index);
      mFs = fs;
    }

    @Override
    protected void applyOperation(int operationIndex, long operationCounter)
        throws IOException, AlluxioException {
      applyOperationNative(
          mFs, mParameters.mOperations[operationIndex], mBasePaths[operationIndex],
          mBasePaths[operationIndex],
          operationCounter, 0);
    }
  }
}
