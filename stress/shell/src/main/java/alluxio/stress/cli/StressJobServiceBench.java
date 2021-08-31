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
import alluxio.cli.fs.command.DistributedLoadCommand;
import alluxio.cli.fs.command.DistributedLoadUtils;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.jobservice.JobServiceBenchParameters;
import alluxio.stress.jobservice.JobServiceBenchTaskResult;
import alluxio.stress.jobservice.JobServiceBenchTaskResultStatistics;
import alluxio.stress.master.MasterBenchTaskResultStatistics;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.BufferUtils;
import alluxio.worker.job.JobMasterClientContext;

import com.beust.jcommander.ParametersDelegate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.HdrHistogram.Histogram;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    createFiles(fileSystem,mParameters.mNumFilesPerDir, mParameters.mNumDirs, mParameters.mFileSize);
  }

  private void createFiles(FileSystem fs, int numFiles, int numDirs, int fileSize)
      throws IOException, AlluxioException {
    CreateFilePOptions options =
        CreateFilePOptions.newBuilder().setRecursive(true).setWriteType(WritePType.THROUGH).build();
    for (int dirId = 0; dirId < numDirs; dirId++) {
      for (int fileId = 0; fileId < numFiles; fileId++) {
        String filePath =
            String.format("%s/%s/%d/%d", mParameters.mBasePath, mBaseParameters.mId, dirId, fileId);
        createByteFile(fs,new AlluxioURI(filePath),options,fileSize);
      }
    }
  }
  private void createByteFile(FileSystem fs, AlluxioURI fileURI, CreateFilePOptions options,
      int len) throws IOException, AlluxioException {
    try (FileOutStream os = fs.createFile(fileURI, options)) {
      byte[] arr = new byte[len];
      for (int k = 0; k < len; k++) {
        arr[k] = (byte) k;
      }
      os.write(arr);
    }
  }

  private void deletePaths(FileSystem fs, String basePath) throws IOException, AlluxioException {
    DeletePOptions options = DeletePOptions.newBuilder().setRecursive(true).build();
    fs.delete(new AlluxioURI(basePath),options);
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
    for (int dirId = 0; dirId < mParameters.mNumDirs; dirId++) {
      String filePath = String.format("%s/%s/%d", mParameters.mBasePath, mBaseParameters.mId, dirId);
      callables.add(new BenchThread(context, filePath));
    }
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);
    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);
    client.close();
    if (!mBaseParameters.mProfileAgent.isEmpty()) {
      context.addAdditionalResult();
    }
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
      applyOperation(mPath);
      long endNs = System.nanoTime();

        mResult.incrementNumSuccess(1);

        // record response times
        long responseTimeNs = endNs - startNs;
        mResponseTimeNs.recordValue(responseTimeNs);

    }

    private void applyOperation(String dirPath) throws IOException, AlluxioException {

      switch (mParameters.mOperation) {
        case DISTRIBUTED_LOAD:
          // send distributed load task to job service and wait for result
          FileSystemContext fsContext =
              FileSystemContext.create(new InstancedConfiguration(ConfigurationUtils.defaults()));
          DistributedLoadCommand cmd = new DistributedLoadCommand(fsContext);
          DistributedLoadUtils.distributedLoad(cmd, new AlluxioURI(dirPath), 1, new HashSet<>(),new HashSet<>(),new HashSet<>(),new HashSet<>());
          // wait for job complete
          return;

        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }
  }
}
