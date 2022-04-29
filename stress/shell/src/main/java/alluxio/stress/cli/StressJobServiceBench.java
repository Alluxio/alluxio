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
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.cli.fs.command.DistributedLoadCommand;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.plan.NoopPlanConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.jobservice.JobServiceBenchParameters;
import alluxio.stress.jobservice.JobServiceBenchTaskResult;
import alluxio.stress.jobservice.JobServiceBenchTaskResultStatistics;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.worker.job.JobMasterClientContext;

import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Job Service stress bench.
 */
public class StressJobServiceBench extends Benchmark<JobServiceBenchTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(StressJobServiceBench.class);
  public static final int MAX_RESPONSE_TIME_BUCKET_INDEX = 0;
  @ParametersDelegate
  private JobServiceBenchParameters mParameters = new JobServiceBenchParameters();
  private FileSystemContext mFsContext;
  private JobMasterClient mJobMasterClient;

  /**
   * Creates instance.
   */
  public StressJobServiceBench() {}

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new StressJobServiceBench());
  }

  @Override
  public void prepare() throws Exception {
    mFsContext =
        FileSystemContext.create(new InstancedConfiguration(ConfigurationUtils.defaults()));
    final ClientContext clientContext = mFsContext.getClientContext();
    mJobMasterClient =
        JobMasterClient.Factory.create(JobMasterClientContext.newBuilder(clientContext).build());
  }

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A benchmarking tool for the job service.",
        "This test will measure the different aspects of job service performance with different "
            + "operations.",
        "",
        "Example:",
        "# This invokes the DistributedLoad jobs to job master",
        "# 256 requests would be sent concurrently to job master",
        "# Each request contains 1000 files with file size 1k",
        "$ bin/alluxio runClass alluxio.stress.cli.StressJobServiceBench --file-size 1k \\"
            + "--files-per-dir 1000 --threads 256 --operation DistributedLoad --cluster",
        ""));
  }

  @Override
  public JobServiceBenchTaskResult runLocal() throws Exception {
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mThreads).create();
    long timeOutMs = FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout);
    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (mBaseParameters.mStartMs == BaseParameters.UNDEFINED_START_MS) {
      startMs = CommonUtils.getCurrentMs() + 1000;
    }
    long endMs = startMs + warmupMs + durationMs;
    RateLimiter rateLimiter = RateLimiter.create(mParameters.mTargetThroughput);
    BenchContext context = new BenchContext(rateLimiter, startMs, endMs);
    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
    for (int dirId = 0; dirId < mParameters.mThreads; dirId++) {
      String filePath =
          String.format("%s/%s/%d", mParameters.mBasePath, mBaseParameters.mId, dirId);
      callables.add(new BenchThread(context, filePath));
    }
    service.invokeAll(callables, timeOutMs, TimeUnit.MILLISECONDS);
    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);
    if (!mBaseParameters.mProfileAgent.isEmpty()) {
      context.addAdditionalResult();
    }
    return context.getResult();
  }

  private final class BenchContext {
    private final RateLimiter mRateLimiter;
    private final long mStartMs;
    private final long mEndMs;
    /**
     * The results. Access must be synchronized for thread safety.
     */
    private JobServiceBenchTaskResult mResult;

    public BenchContext(RateLimiter rateLimiter, long startMs, long endMs) {
      mRateLimiter = rateLimiter;
      mStartMs = startMs;
      mEndMs = endMs;
    }

    public RateLimiter getRateLimiter() {
      return mRateLimiter;
    }

    public long getStartMs() {
      return mStartMs;
    }

    public long getEndMs() {
      return mEndMs;
    }

    public synchronized void mergeThreadResult(JobServiceBenchTaskResult threadResult) {
      if (mResult == null) {
        mResult = threadResult;
        return;
      }
      try {
        mResult.merge(threadResult);
      } catch (Exception e) {
        mResult.addErrorMessage(e.toString());
      }
    }

    @SuppressFBWarnings(value = "DMI_HARDCODED_ABSOLUTE_FILENAME")
    public synchronized void addAdditionalResult() throws IOException {
      if (mResult == null) {
        return;
      }
      Map<String, MethodStatistics> nameStatistics =
          processMethodProfiles(mResult.getRecordStartMs(), mResult.getEndMs(), profileInput -> {
            String method = profileInput.getMethod();
            if (profileInput.getType().contains("RPC")) {
              final int classNameDivider = profileInput.getMethod().lastIndexOf(".");
              method = profileInput.getMethod().substring(classNameDivider + 1);
            }
            return profileInput.getType() + ":" + method;
          });
      for (Map.Entry<String, MethodStatistics> entry : nameStatistics.entrySet()) {
        final JobServiceBenchTaskResultStatistics stats = new JobServiceBenchTaskResultStatistics();
        stats.encodeResponseTimeNsRaw(entry.getValue().getTimeNs());
        stats.mNumSuccess = entry.getValue().getNumSuccess();
        stats.mMaxResponseTimeNs = entry.getValue().getMaxTimeNs();
        mResult.putStatisticsForMethod(entry.getKey(), stats);
      }
    }

    public synchronized JobServiceBenchTaskResult getResult() {
      return mResult;
    }
  }

  private final class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final Histogram mResponseTimeNs;
    private final String mPath;
    private final JobServiceBenchTaskResult mResult = new JobServiceBenchTaskResult();

    private BenchThread(BenchContext context, String path) {
      mContext = context;
      mResponseTimeNs = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
          StressConstants.TIME_HISTOGRAM_PRECISION);
      mPath = path;
    }

    @Override
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
        mResult.addErrorMessage(e.toString());
      }
      // Update local thread result
      mResult.setEndMs(CommonUtils.getCurrentMs());
      mResult.getStatistics().encodeResponseTimeNsRaw(mResponseTimeNs);
      mResult.setParameters(mParameters);
      mResult.setBaseParameters(mBaseParameters);
      // merge local thread result with full result
      mContext.mergeThreadResult(mResult);
      return null;
    }

    private void runInternal() throws Exception {
      long waitMs = mContext.getStartMs() - CommonUtils.getCurrentMs();
      if (waitMs < 0) {
        throw new IllegalStateException(String.format(
            "Thread missed barrier. Set the start time to a later time. start: %d current: %d",
            mContext.getStartMs(), CommonUtils.getCurrentMs()));
      }
      CommonUtils.sleepMs(waitMs);
      applyOperation(mPath);
    }

    private void applyOperation(String dirPath)
        throws IOException, AlluxioException, InterruptedException, TimeoutException {
      switch (mParameters.mOperation) {
        case DISTRIBUTED_LOAD:
          mResult.setRecordStartMs(mContext.getStartMs());
          long startNs = System.nanoTime();
          // send distributed load task to job service and wait for result
          long endNs = runDistributedLoad(dirPath);
          // record response times
          recordResponseTimeInfo(startNs, endNs);
          break;
        case CREATE_FILES:
          FileSystem fileSystem = FileSystem.Factory.create(mFsContext);
          long start = CommonUtils.getCurrentMs();
          deletePath(fileSystem, dirPath);
          long deleteEnd = CommonUtils.getCurrentMs();
          LOG.info("Cleanup delete took: {} s", (deleteEnd - start) / 1000.0);
          long fileSize = FormatUtils.parseSpaceSize(mParameters.mFileSize);
          mResult.setRecordStartMs(mContext.getStartMs());
          createFiles(fileSystem, mParameters.mNumFilesPerDir, dirPath, fileSize);
          long createEnd = CommonUtils.getCurrentMs();
          LOG.info("Create files took: {} s", (createEnd - deleteEnd) / 1000.0);
          break;
        case NO_OP:
          while (true) {
            if (Thread.currentThread().isInterrupted()) {
              break;
            }
            if (CommonUtils.getCurrentMs() >= mContext.getEndMs()) {
              break;
            }
            long recordMs = mContext.getStartMs() + FormatUtils.parseTimeSize(mParameters.mWarmup);
            mResult.setRecordStartMs(recordMs);
            mContext.getRateLimiter().acquire();
            startNs = System.nanoTime();
            runNoop();
            endNs = System.nanoTime();
            long currentMs = CommonUtils.getCurrentMs();
            // Start recording after the warmup
            if (currentMs > recordMs) {
              mResult.incrementNumSuccess(1);
              recordResponseTimeInfo(startNs, endNs);
            }
          }
          break;
        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }

    private void recordResponseTimeInfo(long startNs, long endNs) {
      long responseTimeNs = endNs - startNs;
      mResponseTimeNs.recordValue(responseTimeNs);
      long[] maxResponseTimeNs = mResult.getStatistics().mMaxResponseTimeNs;
      if (responseTimeNs > maxResponseTimeNs[MAX_RESPONSE_TIME_BUCKET_INDEX]) {
        maxResponseTimeNs[MAX_RESPONSE_TIME_BUCKET_INDEX] = responseTimeNs;
      }
    }

    private long runDistributedLoad(String dirPath) throws AlluxioException, IOException {
      int numReplication = 1;
      DistributedLoadCommand cmd = new DistributedLoadCommand(mFsContext);
      long stopTime;
      try {
        long jobControlId = cmd.runDistLoad(new AlluxioURI(dirPath),
                numReplication, mParameters.mBatchSize,
                new HashSet<>(), new HashSet<>(),
                new HashSet<>(), new HashSet<>(), false);
        cmd.waitForCmd(jobControlId);
        stopTime = System.nanoTime();
        cmd.postProcessing(jobControlId);
      } finally {
        mResult.incrementNumSuccess(cmd.getCompletedCount());
      }
      return stopTime;
    }

    private void createFiles(FileSystem fs, int numFiles, String dirPath, long fileSize)
        throws IOException, AlluxioException {
      CreateFilePOptions options = CreateFilePOptions.newBuilder()
          .setRecursive(true).setWriteType(WritePType.THROUGH).build();

      byte[] buf = new byte[Constants.MB];
      Arrays.fill(buf, (byte) 'A');
      for (int fileId = 0; fileId < numFiles; fileId++) {
        String filePath = String.format("%s/%d", dirPath, fileId);
        try (FileOutStream os = fs.createFile(new AlluxioURI(filePath), options)) {
          long bytesWritten = 0;
          while (bytesWritten < fileSize) {
            int toWrite = (int) Math.min(buf.length, fileSize - bytesWritten);
            os.write(buf, 0, toWrite);
            bytesWritten += toWrite;
          }
        }
      }
      mResult.incrementNumSuccess(numFiles);
    }
  }

  private void deletePath(FileSystem fs, String dirPath) throws IOException, AlluxioException {
    AlluxioURI path = new AlluxioURI(dirPath);
    if (fs.exists(path)) {
      DeletePOptions options = DeletePOptions.newBuilder().setRecursive(true).build();
      fs.delete(path, options);
    }
  }

  private void runNoop() throws IOException, InterruptedException, TimeoutException {
    long jobId = mJobMasterClient.run(new NoopPlanConfig());
    // TODO(jianjian): refactor JobTestUtils
    ImmutableSet<Status> statuses =
        ImmutableSet.of(Status.COMPLETED, Status.CANCELED, Status.FAILED);
    final AtomicReference<JobInfo> singleton = new AtomicReference<>();
    CommonUtils.waitFor(
        String.format("job %d to be one of status %s", jobId, Arrays.toString(statuses.toArray())),
        () -> {
          JobInfo info;
          try {
            info = mJobMasterClient.getJobStatus(jobId);
            if (statuses.contains(info.getStatus())) {
              singleton.set(info);
            }
            return statuses.contains(info.getStatus());
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }, WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS));
    JobInfo jobInfo = singleton.get();
    if (jobInfo.getStatus().equals(Status.FAILED)) {
      throw new IOException(jobInfo.getErrorMessage());
    }
  }
}
