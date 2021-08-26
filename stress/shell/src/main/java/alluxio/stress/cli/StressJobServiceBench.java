/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.stress.cli;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.jobservice.JobServiceBenchParameters;
import alluxio.stress.jobservice.JobServiceBenchTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.BufferUtils;
import alluxio.worker.job.JobMasterClientContext;

import com.beust.jcommander.ParametersDelegate;
import org.HdrHistogram.Histogram;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Job Service stress bench
 */
public class StressJobServiceBench extends Benchmark<JobServiceBenchTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(StressJobServiceBench.class);

  @ParametersDelegate
  private JobServiceBenchParameters mParameters = new JobServiceBenchParameters();

  private JobMasterClient[] mJobMasterClients;
  private Path[] mTestDirs;

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
    FileSystemContext fsContext =
        FileSystemContext.create(new InstancedConfiguration(ConfigurationUtils.defaults()));
    FileSystem fileSystem = FileSystem.Factory.create(fsContext);
    long start = CommonUtils.getCurrentMs();
    deletePaths(fileSystem,mParameters.mBasePath);
    long end = CommonUtils.getCurrentMs();
    LOG.info("Cleanup delete took: {} s", (end - start) / 1000.0);

    createFiles(fileSystem,mParameters.mBasePath, mParameters.mNumFilesPerDir, mParameters.mNumDirs,
        mParameters.mFileSize);

    mJobMasterClients = new JobMasterClient[mParameters.mClients];
    for (int i = 0; i < mParameters.mClients; i++) {
      mJobMasterClients[i] = JobMasterClient.Factory
          .create(JobMasterClientContext.newBuilder(ClientContext.create()).build());
    }
  }

  private void createFiles(FileSystem fileSystem, String basePath, int numFiles, int numDirs, int fileSize)
      throws IOException {
    final File baseDir = new File(basePath);
    if (!baseDir.mkdirs()) {
      throw new IOException("Failed to create directory " + baseDir.getAbsolutePath());
    }
    mTestDirs = new Path[numDirs];
    for (int i = 0; i < numDirs; i++) {
      Path dir = Files.createTempDirectory(baseDir.toPath(), "dir");
      mTestDirs[i]=dir;
      byte[] buffer = BufferUtils.getIncreasingByteArray(fileSize);
      for (int j = 0; j < numFiles; j++) {
        String testFilePath = File.createTempFile("temp",null, dir.toFile()).getAbsolutePath();
        BufferUtils.writeBufferToFile(testFilePath, buffer);
      }
    }
  }
  private static void createByteFile(FileSystem fs, AlluxioURI fileURI, CreateFilePOptions options,
      int len) {
    try (FileOutStream os = fs.createFile(fileURI, options)) {
      byte[] arr = new byte[len];
      for (int k = 0; k < len; k++) {
        arr[k] = (byte) k;
      }
      os.write(arr);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(e);
    }
  }

  private void deletePaths(FileSystem fileSystem, String basePath) throws IOException {
    File file = new File(basePath);
    if (!file.exists()) {
      return;
    }
    if (file.isFile()) {
      file.delete();
      return;
    }
    FileUtils.deleteDirectory(file);
  }


  @Override
  public String getBenchDescription() {
    return "";
  }

  @Override
  public JobServiceBenchTaskResult runLocal() throws Exception {
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mNumDirs).create();

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (mBaseParameters.mStartMs == BaseParameters.UNDEFINED_START_MS) {
      startMs = CommonUtils.getCurrentMs() + 1000;
    }
    long endMs = startMs + warmupMs + durationMs;
    JobMasterClient client = JobMasterClient.Factory
        .create(JobMasterClientContext.newBuilder(ClientContext.create()).build());
    BenchContext context = new BenchContext(startMs, endMs);
    List<Callable<Void>> callables = new ArrayList<>(mParameters.mNumDirs);
    for (int i = 0; i < mParameters.mNumDirs; i++) {

      callables.add(new BenchThread(context, mTestDirs[i], client));
    }
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);
    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);
    client.close();
    return context.getResult();
  }


  private final class BenchContext {
    private final long mStartMs;
    private final long mEndMs;
    private final AtomicLong mCounter;

    /**
     * The results. Access must be synchronized for thread safety.
     */
    private JobServiceBenchTaskResult mResult;

    public BenchContext(long startMs, long endMs) {
      mStartMs = startMs;
      mEndMs = endMs;
      mCounter = new AtomicLong();
    }


    public long getStartMs() {
      return mStartMs;
    }

    public long getEndMs() {
      return mEndMs;
    }

    public AtomicLong getCounter() {
      return mCounter;
    }

    public synchronized void mergeThreadResult(JobServiceBenchTaskResult threadResult) {
      if (mResult == null) {
        mResult = threadResult;
        return;
      }
      try {
        mResult.merge(threadResult);
      } catch (Exception e) {
        mResult.addErrorMessage(e.getMessage());
      }
    }

    public synchronized JobServiceBenchTaskResult getResult() {
      return mResult;
    }
  }


  private final class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final Histogram mResponseTimeNs;
    private final JobMasterClient mJobMasterClient;
    private final Path mPath;
    private final JobServiceBenchTaskResult mResult = new JobServiceBenchTaskResult();

    private BenchThread(BenchContext context, Path path, JobMasterClient client) {
      mContext = context;
      mResponseTimeNs = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
          StressConstants.TIME_HISTOGRAM_PRECISION);
      mJobMasterClient = client;
      mPath = path;
    }

    @Override
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
        mResult.addErrorMessage(e.getMessage());
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
      // When to start recording measurements
      long recordMs = mContext.getStartMs() + FormatUtils.parseTimeSize(mParameters.mWarmup);
      mResult.setRecordStartMs(recordMs);

      long waitMs = mContext.getStartMs() - CommonUtils.getCurrentMs();
      if (waitMs < 0) {
        throw new IllegalStateException(String.format(
            "Thread missed barrier. Set the start time to a later time. start: %d current: %d",
            mContext.getStartMs(), CommonUtils.getCurrentMs()));
      }
      CommonUtils.sleepMs(waitMs);
      long startNs = System.nanoTime();
      applyOperation();
      while (!Thread.currentThread().isInterrupted()&& CommonUtils.getCurrentMs() < mContext.getEndMs()) {

        JobInfo status = mJobMasterClient.getJobStatus();
        if (status.getStatus() == Status.COMPLETED){
        long endNs = System.nanoTime();

        long currentMs = CommonUtils.getCurrentMs();
        // Start recording after the warmup
        if (currentMs > recordMs) {
          mResult.incrementNumSuccess(1);

          // record response times
          long responseTimeNs = endNs - startNs;
          mResponseTimeNs.recordValue(responseTimeNs);}
        }
      }
    }

    private void applyOperation() throws IOException {
      long counter = mContext.getCounter().getAndIncrement();

      switch (mParameters.mOperation) {
        case DISTRIBUTED_LOAD:
          // send distributed load task to job service and wait for result

          // wait for job complete
          return;

        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }
  }
}
