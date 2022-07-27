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

import alluxio.AlluxioURI;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.SetAttributePOptions;
import alluxio.hadoop.HadoopConfigurationUtils;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.common.FileSystemClientType;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.stress.master.MasterBenchTaskResult;
import alluxio.stress.master.MasterBenchTaskResultStatistics;
import alluxio.stress.master.Operation;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import org.HdrHistogram.Histogram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single node stress test.
 */
// TODO(jiacheng): avoid the implicit casts and @SuppressFBWarnings
public class StressMasterBench extends AbstractStressBench<MasterBenchTaskResult,
    MasterBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(StressMasterBench.class);

  private byte[] mFiledata;

  /** Cached FS instances. */
  private FileSystem[] mCachedFs;

  /** In case the Alluxio Native API is used,  use the following instead. */
  protected alluxio.client.file.FileSystem[] mCachedNativeFs;
  /* Directories where the stress bench creates files depending on the --operation chosen. */
  protected final String mDirsDir = "dirs";
  protected final String mFilesDir = "files";
  protected final String mFixedDir = "fixed";

  /**
   * Creates instance.
   */
  public StressMasterBench() {
    mParameters = new MasterBenchParameters();
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new StressMasterBench());
  }

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A benchmarking tool to measure the master performance of Alluxio",
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
    LOG.info("shuaiwuyue");
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
              || op == Operation.CREATE_DIR
              || op == Operation.CRURD
              || op == Operation.CREATE_DELETE_FILE) {
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
    // set hdfs conf for all test clients
    Configuration hdfsConf = new Configuration();
    // do not cache these clients
    hdfsConf.set(
        String.format("fs.%s.impl.disable.cache", (new URI(mParameters.mBasePath)).getScheme()),
        "true");
    for (Map.Entry<String, String> entry : mParameters.mConf.entrySet()) {
      hdfsConf.set(entry.getKey(), entry.getValue());
    }

    hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, mParameters.mWriteType);

    if (mParameters.mClientType == FileSystemClientType.ALLUXIO_HDFS) {
      LOG.info("Using ALLUXIO HDFS Compatible API to perform the test.");
      mCachedFs = new FileSystem[mParameters.mClients];
      for (int i = 0; i < mCachedFs.length; i++) {
        mCachedFs[i] = FileSystem.get(new URI(mParameters.mBasePaths[0]), hdfsConf);
      }
    } else {
      LOG.info("Using ALLUXIO Native API to perform the test.");
      InstancedConfiguration alluxioProperties = alluxio.conf.Configuration.copyGlobal();
      alluxioProperties.merge(HadoopConfigurationUtils.getConfigurationFromHadoop(hdfsConf),
          Source.RUNTIME);

      mCachedNativeFs = new alluxio.client.file.FileSystem[mParameters.mClients];
      for (int i = 0; i < mCachedNativeFs.length; i++) {
        mCachedNativeFs[i] = alluxio.client.file.FileSystem.Factory
            .create(alluxioProperties);
      }
    }
  }

  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  private void deletePaths(FileSystem fs, Path basePath) throws Exception {
    // the base dir has sub directories per task id
    if (!fs.exists(basePath)) {
      return;
    }

    FileStatus[] subDirs = fs.listStatus(basePath);

    if (subDirs.length == 0) {
      return;
    }

    if (mParameters.mClientType == FileSystemClientType.ALLUXIO_HDFS) {
      fs.delete(basePath, true);
      if (fs.exists(basePath)) {
        throw new UnexpectedAlluxioException(String.format("Unable to delete the files"
            + " in path %s.Please confirm whether it is HDFS file system."
            + " You may need to modify `--client-type` parameter", basePath));
      }
      return;
    }

    long batchSize = 50_000;
    int deleteThreads = 256;
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-delete-thread", deleteThreads).create();
    if (subDirs[0].isDirectory()) {
      for (FileStatus subDir : subDirs) {
        LOG.info("Cleaning up all files in: {}", subDir.getPath());
        AtomicLong globalCounter = new AtomicLong();
        long runningLimit = 0;

        // delete individual files in batches, to avoid the recursive-delete problem
        while (!Thread.currentThread().isInterrupted()) {
          AtomicLong success = new AtomicLong();
          runningLimit += batchSize;
          long limit = runningLimit;

          List<Callable<Void>> callables = new ArrayList<>(deleteThreads);
          for (int i = 0; i < deleteThreads; i++) {
            callables.add(() -> {
              while (!Thread.currentThread().isInterrupted()) {
                long counter = globalCounter.getAndIncrement();
                if (counter >= limit) {
                  globalCounter.getAndDecrement();
                  return null;
                }
                Path deletePath = new Path(subDir.getPath(), Long.toString(counter));
                if (fs.delete(deletePath, true)) {
                  success.getAndIncrement();
                }
              }
              return null;
            });
          }
          // This may cancel some remaining threads, but that is fine, because any remaining paths
          // will be taken care of during the final recursive delete.
          service.invokeAll(callables, 1, TimeUnit.MINUTES);

          if (success.get() == 0) {
            // stop deleting one-by-one if none of the batch succeeded.
            break;
          }
          LOG.info("Removed {} files", success.get());
        }
      }
    }
    else {
      LOG.info("Cleaning up all files in: {}", basePath);
      AtomicLong globalCounter = new AtomicLong();
      long runningLimit = 0;

      // delete individual files in batches, to avoid the recursive-delete problem
      while (!Thread.currentThread().isInterrupted()) {
        AtomicLong success = new AtomicLong();
        runningLimit += batchSize;
        long limit = runningLimit;

        List<Callable<Void>> callables = new ArrayList<>(deleteThreads);
        for (int i = 0; i < deleteThreads; i++) {
          callables.add(() -> {
            while (!Thread.currentThread().isInterrupted()) {
              long counter = globalCounter.getAndIncrement();
              if (counter >= limit) {
                globalCounter.getAndDecrement();
                return null;
              }
              if (counter <= subDirs.length) {
                Path deletePath = subDirs[(int) counter].getPath();
                if (fs.delete(deletePath, true)) {
                  success.getAndIncrement();
                }
              } else {
                return null;
              }
            }
            return null;
          });
        }
        // This may cancel some remaining threads, but that is fine, because any remaining paths
        // will be taken care of during the final recursive delete.
        service.invokeAll(callables, 1, TimeUnit.MINUTES);

        if (success.get() == 0) {
          // stop deleting one-by-one if none of the batch succeeded.
          break;
        }
        LOG.info("Removed {} files", success.get());
      }
    }

    service.shutdownNow();
    service.awaitTermination(10, TimeUnit.SECONDS);

    // Cleanup the rest recursively, which should be empty or much smaller than the full tree.
    LOG.info("Deleting base directory: {}", basePath);
    fs.delete(basePath, true);
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public MasterBenchTaskResult runLocal() throws Exception {
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mThreads).create();
    RateLimiter[] rls;
    if (mParameters.mTargetThroughputs != null) {
      int sum = 0;
      rls = new RateLimiter[mParameters.mTargetThroughputs.length+1];
      for (int i = 0; i < mParameters.mTargetThroughputs.length; i++) {
        rls[i] = RateLimiter.create(mParameters.mTargetThroughputs[i]);
        sum += mParameters.mTargetThroughputs[i];
      }
      rls[rls.length-1] = RateLimiter.create(sum);
    } else {
      rls = new RateLimiter[]{RateLimiter.create(mParameters.mTargetThroughput)};
    }
    long fileSize = FormatUtils.parseSpaceSize(mParameters.mCreateFileSize);
    mFiledata = new byte[(int) Math.min(fileSize, StressConstants.WRITE_FILE_ONCE_MAX_BYTES)];
    Arrays.fill(mFiledata, (byte) 0x7A);

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (mBaseParameters.mStartMs == BaseParameters.UNDEFINED_START_MS) {
      startMs = CommonUtils.getCurrentMs() + 1000;
    }
    long endMs = startMs + warmupMs + durationMs;
    BenchContext context = new
        BenchContext(rls, startMs, endMs, mParameters.mOperations, mParameters.mCountersOffset);
    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
    for (int i = 0; i < mParameters.mThreads; i++) {
      callables.add(getBenchThread(context, i));
    }
    LOG.info("Starting {} bench threads", callables.size());
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);
    LOG.info("Bench threads finished");

    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);

    if (!mBaseParameters.mProfileAgent.isEmpty()) {
      context.addAdditionalResult();
    }

    return context.getResult();
  }

  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  private BenchThread getBenchThread(BenchContext context, int index) {
    if (mParameters.mClientType == FileSystemClientType.ALLUXIO_HDFS) {
      return new AlluxioHDFSBenchThread(context, mCachedFs[index % mCachedFs.length]);
    }

    return new AlluxioNativeBenchThread(context, mCachedNativeFs[index % mCachedNativeFs.length]);
  }

  private final class BenchContext {
    private final RateLimiter[] mRateLimiters;
    private final long mStartMs;
    private final long mEndMs;
    private final AtomicLong[] mOperationCounter;
    private final AtomicLong mTotalCounter;
    private final Path[] mBasePath;
    private final Path[] mFixedBasePath;

    /** The results. Access must be synchronized for thread safety. */
    private MasterBenchTaskResult mResult;

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    public BenchContext(RateLimiter[] rateLimiters,
                        long startMs, long endMs, Operation[] operations, long[] counterOffset) {
      mRateLimiters = rateLimiters;
      mStartMs = startMs;
      mEndMs = endMs;
      mOperationCounter = new AtomicLong[operations.length];
      mTotalCounter = new AtomicLong();
      mBasePath = new Path[operations.length];
      mFixedBasePath = new Path[operations.length];
      for (int i = 0; i < operations.length; i++) {
        mOperationCounter[i] = new AtomicLong(counterOffset[i]);
        if (operations[i] == Operation.CREATE_DIR) {
          mBasePath[i] =
              new Path(PathUtils.concatPath(mParameters.mBasePaths[i],
                  mDirsDir));
        } else {
          mBasePath[i] =
              new Path(PathUtils.concatPath(mParameters.mBasePaths[i],
                  mFilesDir));
        }
        if (!mParameters.mSingleDir) {
          mBasePath[i] = new Path(mBasePath[i], mBaseParameters.mId);
        }
        mFixedBasePath[i] = new Path(mBasePath[i], mFixedDir);
        LOG.info("BenchContext: basePath: {}, fixedBasePath: {}", mBasePath[i], mFixedBasePath[i]);
      }
    }

    public RateLimiter[] getRateLimiters() {
      return mRateLimiters;
    }

    public long getStartMs() {
      return mStartMs;
    }

    public long getEndMs() {
      return mEndMs;
    }

    public AtomicLong getOperationCounter(int i) {
      return mOperationCounter[i];
    }

    public AtomicLong getTotalCounter() {
      return mTotalCounter;
    }

    public Path[] getBasePath() {
      return mBasePath;
    }

    public Path[] getFixedBasePath() {
      return mFixedBasePath;
    }

    public synchronized void mergeThreadResult(MasterBenchTaskResult threadResult) {
      if (mResult == null) {
        mResult = threadResult;
        return;
      }
      try {
        mResult.merge(threadResult);
      } catch (Exception e) {
        LOG.warn("Exception during result merge", e);
        mResult.addErrorMessage(e.getMessage());
      }
    }

    @SuppressFBWarnings(value = "DMI_HARDCODED_ABSOLUTE_FILENAME")
    public synchronized void addAdditionalResult() throws IOException {
      if (mResult == null) {
        return;
      }
      Map<String, MethodStatistics> nameStatistics =
          processMethodProfiles(mResult.getRecordStartMs(), mResult.getEndMs(),
              profileInput -> {
              String method = profileInput.getMethod();
              if (profileInput.getType().contains("RPC")) {
                final int classNameDivider = profileInput.getMethod().lastIndexOf(".");
                method = profileInput.getMethod().substring(classNameDivider + 1);
              }
              return profileInput.getType() + ":" + method;
            });

      for (Map.Entry<String, MethodStatistics> entry : nameStatistics.entrySet()) {
        final MasterBenchTaskResultStatistics stats = new MasterBenchTaskResultStatistics();
        stats.encodeResponseTimeNsRaw(entry.getValue().getTimeNs());
        stats.mNumSuccess = entry.getValue().getNumSuccess();
        stats.mMaxResponseTimeNs = entry.getValue().getMaxTimeNs();
        mResult.putStatisticsForMethod(entry.getKey(), stats);
      }
    }

    public synchronized MasterBenchTaskResult getResult() {
      return mResult;
    }
  }

  protected abstract class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final Histogram[] mResponseTimeNs;
    protected final Path[] mBasePaths;
    protected final Path[] mFixedBasePaths;
    private final MasterBenchTaskResult mResult;

    private BenchThread(BenchContext context) {
      mContext = context;
      mResponseTimeNs = new Histogram[mParameters.mOperations.length];
      mBasePaths = mContext.getBasePath();
      mFixedBasePaths = mContext.getFixedBasePath();
      mResult = new MasterBenchTaskResult(mParameters.mOperations.length);
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        mResponseTimeNs[i] = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
            StressConstants.TIME_HISTOGRAM_PRECISION);
      }
    }

    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    public Void call() {
      try {
        if (mParameters.mOperationsRatio != null) {
          runInternalBasedOperationRatio();
        } else if (mParameters.mThreadsRatio != null) {
          runInternalBasedThreadsRatio();
        } else {
          runInternalBasedTargetThroughput();
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
    private void runInternalBasedOperationRatio() throws Exception {
      RateLimiter rl = mContext.getRateLimiters()[0];
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
//      Thread.currentThread().getId()
      CommonUtils.sleepMs(waitMs);
      Random random = new Random();
      long totalCount;
      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        if (!useStopCount && CommonUtils.getCurrentMs() >= mContext.getEndMs()) {
          break;
        }
        totalCount = mContext.getTotalCounter().getAndIncrement();
        if (useStopCount && totalCount >= mParameters.mStopCount) {
          break;
        }
        //select an operation
        double r = random.nextDouble();
        int operationIndex = 0;

        for (int i = 0; i < mParameters.mOperations.length; i++) {
          if (ratioArray[i] >= r) {
            operationIndex = i;
            break;
          }
        }
        long operationCount;
        operationCount = mContext.getOperationCounter(operationIndex).getAndIncrement();
        rl.acquire();
        long startNs = System.nanoTime();
        applyOperation(operationCount, mParameters.mOperations[operationIndex], operationIndex);
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

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runInternalBasedThreadsRatio() throws Exception {
      String[] components = Thread.currentThread().getName().split("-");
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

      int threadID = Integer.parseInt(components[components.length - 1 ]);
      int operationIndex = 0;
      for (int i = 0; i < mParameters.mOperations.length; i++) {
        if (ratioArray[i] >= threadID) {
          operationIndex = i;
          break;
        }
      }
      RateLimiter rls = mContext.getRateLimiters()[operationIndex];
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

      long totalCount;
      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        if (!useStopCount && CommonUtils.getCurrentMs() >= mContext.getEndMs()) {
          break;
        }
        totalCount = mContext.getTotalCounter().getAndIncrement();
        if (useStopCount && totalCount >= mParameters.mStopCount) {
          break;
        }

        long operationCount;
        operationCount = mContext.getOperationCounter(operationIndex).getAndIncrement();
        rls.acquire();
        long startNs = System.nanoTime();
        applyOperation(operationCount, mParameters.mOperations[operationIndex], operationIndex);
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

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runInternalBasedTargetThroughput() throws Exception {
      RateLimiter[] rls = mContext.getRateLimiters();
      RateLimiter controlRls = rls[rls.length-1];
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

      long totalCount;
      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        if (!useStopCount && CommonUtils.getCurrentMs() >= mContext.getEndMs()) {
          break;
        }
        totalCount = mContext.getTotalCounter().getAndIncrement();
        if (useStopCount && totalCount >= mParameters.mStopCount) {
          break;
        }
        controlRls.acquire();
        int operationIndex = 0;
        while (!rls[operationIndex].tryAcquire()) {
          operationIndex++;
          operationIndex %= mParameters.mOperations.length;
        }
        long operationCount;
        operationCount = mContext.getOperationCounter(operationIndex).getAndIncrement();
        long startNs = System.nanoTime();
        applyOperation(operationCount, mParameters.mOperations[operationIndex], operationIndex);
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

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    protected abstract void applyOperation(long counter,
         Operation operation, int operationIndex) throws IOException, AlluxioException;
  }

  private final class AlluxioHDFSBenchThread extends BenchThread {
    private final FileSystem mFs;

    private AlluxioHDFSBenchThread(BenchContext context, FileSystem fs) {
      super(context);
      mFs = fs;
    }

    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    protected void applyOperation(long counter,
        Operation operation, int opIdx) throws IOException {
      Path path;
      switch (operation) {
        case CREATE_DIR:
          if (counter < mParameters.mFixedCounts[opIdx]) {
            path = new Path(mFixedBasePaths[opIdx], Long.toString(counter));
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          mFs.mkdirs(path);
          break;
        case CREATE_FILE:
          if (counter < mParameters.mFixedCounts[opIdx]) {
            path = new Path(mFixedBasePaths[opIdx], Long.toString(counter));
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          long fileSize = FormatUtils.parseSpaceSize(mParameters.mCreateFileSize);
          try (FSDataOutputStream stream = mFs.create(path)) {
            for (long i = 0; i < fileSize; i += StressConstants.WRITE_FILE_ONCE_MAX_BYTES) {
              stream.write(mFiledata, 0,
                  (int) Math.min(StressConstants.WRITE_FILE_ONCE_MAX_BYTES, fileSize - i));
            }
          }
          break;
        case GET_BLOCK_LOCATIONS:
          counter = counter % mParameters.mFixedCounts[opIdx];
          path = new Path(mFixedBasePaths[opIdx], Long.toString(counter));
          mFs.getFileBlockLocations(path, 0, 0);
          break;
        case GET_FILE_STATUS:
          counter = counter % mParameters.mFixedCounts[opIdx];
          path = new Path(mFixedBasePaths[opIdx], Long.toString(counter));
          mFs.getFileStatus(path);
          break;
        case LIST_DIR:
          mFs.listStatus(mFixedBasePaths[opIdx]);
          break;
        case LIST_DIR_LOCATED:
          RemoteIterator<LocatedFileStatus> it = mFs.listLocatedStatus(mFixedBasePaths[opIdx]);
          int listedFiles = 0;
          while (it.hasNext()) {
            it.next();
            listedFiles++;
          }
          if (listedFiles != mParameters.mFixedCounts[opIdx]) {
            throw new IOException(String
                .format("listing located `%s` expected %d files but got %d files",
                    mFixedBasePaths[opIdx],
                    mParameters.mFixedCounts[opIdx], listedFiles));
          }
          break;
        case OPEN_FILE:
          counter = counter % mParameters.mFixedCounts[opIdx];
          path = new Path(mFixedBasePaths[opIdx], Long.toString(counter));
          mFs.open(path).close();
          break;
        case RENAME_FILE:
          if (counter < mParameters.mFixedCounts[opIdx]) {
            path = new Path(mFixedBasePaths[opIdx], Long.toString(counter));
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          Path dst = new Path(path + "-renamed");
          if (!mFs.rename(path, dst)) {
            throw new IOException(String.format("Failed to rename (%s) to (%s)", path, dst));
          }
          break;
        case DELETE_FILE:
          if (counter < mParameters.mFixedCounts[opIdx]) {
            path = new Path(mFixedBasePaths[opIdx], Long.toString(counter));
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          if (!mFs.delete(path, false)) {
            throw new IOException(String.format("Failed to delete (%s)", path));
          }
          break;
        default:
          throw new IllegalStateException("Unknown operation: " + operation);
      }
    }
  }

  private final class AlluxioNativeBenchThread extends BenchThread {
    private final alluxio.client.file.FileSystem mFs;

    private AlluxioNativeBenchThread(BenchContext context, alluxio.client.file.FileSystem fs) {
      super(context);
      mFs = fs;
    }

    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    protected void applyOperation(long counter, Operation operation, int opIdx)
        throws IOException, AlluxioException {
      Path path;
      switch (operation) {
        case CREATE_DIR:
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          mFs.createDirectory(new AlluxioURI(path.toString()),
              CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
          break;
        case CREATE_FILE:
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          long fileSize = FormatUtils.parseSpaceSize(mParameters.mCreateFileSize);
          try (FileOutStream stream = mFs.createFile(new AlluxioURI(path.toString()),
              CreateFilePOptions.newBuilder().setRecursive(true).build())) {
            for (long i = 0; i < fileSize; i += StressConstants.WRITE_FILE_ONCE_MAX_BYTES) {
              stream.write(mFiledata, 0,
                  (int) Math.min(StressConstants.WRITE_FILE_ONCE_MAX_BYTES, fileSize - i));
            }
          }
          break;
        case GET_BLOCK_LOCATIONS:
          counter = counter % mParameters.mFixedCounts[opIdx];
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          mFs.getBlockLocations(new AlluxioURI(path.toString()));
          break;
        case GET_FILE_STATUS:
          counter = counter % mParameters.mFixedCounts[opIdx];
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          mFs.getStatus(new AlluxioURI(path.toString()));
          break;
        case LIST_DIR:
          mFs.listStatus(new AlluxioURI(mBasePaths[opIdx].toString()));
          break;
        case LIST_DIR_LOCATED:
          throw new UnsupportedOperationException("LIST_DIR_LOCATED is not supported!");
        case OPEN_FILE:
          counter = counter % mParameters.mFixedCounts[opIdx];
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          mFs.openFile(new AlluxioURI(path.toString())).close();
          break;
        case RENAME_FILE:
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          Path dst = new Path(path + "-renamed");
          mFs.rename(new AlluxioURI(path.toString()), new AlluxioURI(dst.toString()));
          break;
        case DELETE_FILE:
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }

          mFs.delete(new AlluxioURI(path.toString()),
              DeletePOptions.newBuilder().setRecursive(false).build());
          break;
        case CRURD:
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          AlluxioURI alluxioPath = new AlluxioURI(path.toString());
          PMode mode = PMode.newBuilder()
              .setOwnerBits(Bits.READ_WRITE)
              .setGroupBits(Bits.READ_WRITE)
              .setGroupBits(Bits.READ_WRITE)
              .setOtherBits(Bits.READ_WRITE).build();
          mFs.createFile(alluxioPath,
              CreateFilePOptions.newBuilder().setRecursive(true).setMode(mode).build()).close();
          URIStatus us = mFs.getStatus(new AlluxioURI(path.toString()));
          if (us.getMode() != 438) {
            throw new IOException("file inconstancy");
          }
          mode = PMode.newBuilder()
              .setOwnerBits(Bits.ALL)
              .setGroupBits(Bits.ALL)
              .setGroupBits(Bits.ALL)
              .setOtherBits(Bits.ALL).build();
          mFs.setAttribute(alluxioPath, SetAttributePOptions.newBuilder().setMode(mode).build());
          us = mFs.getStatus(new AlluxioURI(path.toString()));
          if (us.getMode() != 511) {
            throw new IOException("file inconstancy");
          }
          mFs.delete(alluxioPath);
          if (mFs.exists(alluxioPath)) {
            throw new IOException("file inconstancy");
          }
          break;
        case CREATE_DELETE_FILE:
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          AlluxioURI filePath = new AlluxioURI(path.toString());
          mFs.createFile(filePath).close();
          mFs.delete(filePath);
          break;
        case UPDATE_FILE_MODE:
          counter = counter % mParameters.mFixedCounts[opIdx];
          if (mParameters.mSingleDir) {
            path = new Path(mBasePaths[opIdx], mBaseParameters.mId + "-" + counter);
          } else {
            path = new Path(mBasePaths[opIdx], Long.toString(counter));
          }
          mFs.setAttribute(new AlluxioURI(path.toString()), SetAttributePOptions.newBuilder()
              .setMode(PMode.newBuilder()
                  .setOwnerBits(Bits.ALL)
                  .setGroupBits(Bits.ALL)
                  .setGroupBits(Bits.ALL)
                  .setOtherBits(Bits.ALL).build()).build());
          break;
        default:
          throw new IllegalStateException("Unknown operation: " + operation);
      }
    }
  }
}
