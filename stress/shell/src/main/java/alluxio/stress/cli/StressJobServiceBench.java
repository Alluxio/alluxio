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
import alluxio.AlluxioURI;
import alluxio.cli.fs.command.DistributedLoadCommand;
import alluxio.cli.fs.command.DistributedLoadUtils;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.WritePType;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.jobservice.JobServiceBenchParameters;
import alluxio.stress.jobservice.JobServiceBenchTaskResult;
import alluxio.stress.jobservice.JobServiceBenchTaskResultStatistics;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.beust.jcommander.ParametersDelegate;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Job Service stress bench.
 */
public class StressJobServiceBench extends Benchmark<JobServiceBenchTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(StressJobServiceBench.class);
  public static final int MAX_RESPONSE_TIME_BUCKET_INDEX = 0;
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
  public void prepare() throws Exception {}

  @Override
  public String getBenchDescription() {
    return "";
  }

  @Override
  public JobServiceBenchTaskResult runLocal() throws Exception {
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mNumDirs).create();
    long timeOutMs = FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout);
    long startMs = mBaseParameters.mStartMs;
    if (mBaseParameters.mStartMs == BaseParameters.UNDEFINED_START_MS) {
      startMs = CommonUtils.getCurrentMs() + 1000;
    }
    BenchContext context = new BenchContext(startMs);
    List<Callable<Void>> callables = new ArrayList<>(mParameters.mNumDirs);
    for (int dirId = 0; dirId < mParameters.mNumDirs; dirId++) {
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
    private final long mStartMs;

    /**
     * The results. Access must be synchronized for thread safety.
     */
    private JobServiceBenchTaskResult mResult;

    public BenchContext(long startMs) {
      mStartMs = startMs;
    }

    public long getStartMs() {
      return mStartMs;
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
      mResult.setRecordStartMs(mContext.getStartMs());
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

      // record response times
      long responseTimeNs = endNs - startNs;
      mResponseTimeNs.recordValue(responseTimeNs);
      long[] maxResponseTimeNs = mResult.getStatistics().mMaxResponseTimeNs;
      if (responseTimeNs > maxResponseTimeNs[MAX_RESPONSE_TIME_BUCKET_INDEX]) {
        maxResponseTimeNs[MAX_RESPONSE_TIME_BUCKET_INDEX] = responseTimeNs;
      }
    }

    private void applyOperation(String dirPath) throws IOException, AlluxioException {
      FileSystemContext fsContext =
          FileSystemContext.create(new InstancedConfiguration(ConfigurationUtils.defaults()));
      switch (mParameters.mOperation) {
        case DISTRIBUTED_LOAD:
          // send distributed load task to job service and wait for result
          int numReplication = 1;

          DistributedLoadCommand cmd = new DistributedLoadCommand(fsContext);
          try {
            DistributedLoadUtils.distributedLoad(cmd, new AlluxioURI(dirPath), numReplication,
                new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), false);
          } finally {
            mResult.incrementNumSuccess(cmd.getCompletedCount());
          }
          break;
        case CREATE_FILES:
          FileSystem fileSystem = FileSystem.Factory.create(fsContext);
          long start = CommonUtils.getCurrentMs();
          deletePath(fileSystem, dirPath);
          long deleteEnd = CommonUtils.getCurrentMs();
          LOG.info("Cleanup delete took: {} s", (deleteEnd - start) / 1000.0);
          int fileSize = (int) FormatUtils.parseSpaceSize(mParameters.mFileSize);
          createFiles(fileSystem, mParameters.mNumFilesPerDir, dirPath, fileSize);
          long createEnd = CommonUtils.getCurrentMs();
          LOG.info("Create files took: {} s", (createEnd - deleteEnd) / 1000.0);
          break;
        case NO_OP:

        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }

    private void createFiles(FileSystem fs, int numFiles, String dirPath, int fileSize)
        throws IOException, AlluxioException {
      CreateFilePOptions options = CreateFilePOptions.newBuilder().setRecursive(true)
          .setWriteType(WritePType.THROUGH).build();

      for (int fileId = 0; fileId < numFiles; fileId++) {
        String filePath = String.format("%s/%d", dirPath, fileId);
        createByteFile(fs, new AlluxioURI(filePath), options, fileSize);
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

    private void deletePath(FileSystem fs, String dirPath) throws IOException, AlluxioException {
      AlluxioURI path = new AlluxioURI(dirPath);
      if (fs.exists(path)) {
        DeletePOptions options = DeletePOptions.newBuilder().setRecursive(true).build();
        fs.delete(path, options);
      }
    }
  }
}
