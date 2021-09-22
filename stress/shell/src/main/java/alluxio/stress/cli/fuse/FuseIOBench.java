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

package alluxio.stress.cli.fuse;

import alluxio.annotation.SuppressFBWarnings;
import alluxio.ClientContext;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.Constants;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.common.SummaryStatistics;
import alluxio.stress.fuse.FuseIOOperation;
import alluxio.stress.fuse.FuseIOParameters;
import alluxio.stress.fuse.FuseIOTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.worker.job.JobMasterClientContext;

import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Single node stress test.
 */
public class FuseIOBench extends Benchmark<FuseIOTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(FuseIOBench.class);
  private static final String TEST_DIR_STRING_FORMAT = "%s/%s/dir-%d";
  private static final String TEST_FILE_STRING_FORMAT = "%s/%s/dir-%d/file-%d";

  @ParametersDelegate
  private FuseIOParameters mParameters = new FuseIOParameters();

  /** Names of the directories created for the test, also unique ids of the job workers. */
  private List<String> mJobWorkerDirNames;
  /** 0-based id of this job worker. */
  private int mJobWorkerZeroBasedId;
  /** Set to true after the first barrier is passed. */
  private volatile boolean mStartBarrierPassed = false;

  /**
   * Creates instance.
   */
  public FuseIOBench() {
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new FuseIOBench());
  }

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A stress bench for testing the reading throughput of Fuse-based POSIX API.",
        "To run the test, data must be written first by executing \"Write\" operation, then "
            + "run \"Read\" operation to test the reading throughput. The three different options "
            + "of read are: ",
        "LocalRead: Each job worker, or client, will read the files it wrote through local Fuse "
            + "mount point.",
        "RemoteRead: Each job worker will evenly read the files written by other job workers "
            + "through local Fuse mount point.",
        "ClusterRead: Read <numAllFiles>/<numJobWorker> number of files evenly from all "
            + "directories created by all job workers through local Fuse mount point.",
        "Optionally one can set alluxio.user.metadata.cache.enabled=true when mounting Alluxio "
            + "Fuse and run \"ListFile\" before \"Read\" to cache the metadata of the test files "
            + "and eliminate the effect of metadata operations while getting the reading "
            + "throughput data.",
        "Note that \"--operation\" is required, \"--local-path\" can be a local filesystem "
            + "path or a mounted Fuse path, and non-cluster mode only supports local read.",
        "",
        "Example:",
        "# The test will be run in cluster mode using job service",
        "# The test data will be written to /mnt/alluxio-fuse/FuseIOTest",
        "# Files will be evenly distributed into 32 directories, each contains 10 files of "
            + "size 100 MB. 32 threads of each worker will be used to generate the files",
        "# Metadata of the test files will be cached",
        "# 16 threads of each worker will be used for testing the reading throughput with "
            + "ClusterRead.",
        "# 5 seconds of warmup time and 30 seconds of actual reading test time",
        "$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation Write \\",
        "--local-path /mnt/alluxio-fuse/FuseIOTest --num-dirs 32 --num-files-per-dir 10 \\",
        "--file-size 100m --threads 32 --cluster",
        "$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation ListFile \\",
        "--local-path /mnt/alluxio-fuse/FuseIOTest",
        "$ bin/alluxio runClass alluxio.stress.cli.fuse.FuseIOBench --operation ClusterRead \\",
        "--local-path /mnt/alluxio-fuse/FuseIOTest --num-dirs 32 --num-files-per-dir 10 \\",
        "--file-size 100m --threads 16 --warmup 5s --duration 30s --cluster",
        ""
    ));
  }

  @Override
  public void prepare() throws Exception {
    if (mBaseParameters.mCluster) {
      // For cluster mode, this function is called once before the job is submitted to the job
      // service. Nothing should be done, otherwise the bench will break.
      return;
    }
    if (mParameters.mThreads > mParameters.mNumDirs) {
      throw new IllegalArgumentException(String.format(
          "Some of the threads are not being used. Please set the number of directories to "
              + "be at least the number of threads, preferably a multiple of it."
      ));
    }
    File localPath = new File(mParameters.mLocalPath);

    if (mParameters.mOperation == FuseIOOperation.WRITE) {
      LOG.warn("Cannot write repeatedly, so warmup is not possible. Setting warmup to 0s.");
      mParameters.mWarmup = "0s";
      for (int i = 0; i < mParameters.mNumDirs; i++) {
        Files.createDirectories(Paths.get(String.format(
            TEST_DIR_STRING_FORMAT, mParameters.mLocalPath, mBaseParameters.mId, i)));
      }
      return;
    }
    if ((mParameters.mOperation == FuseIOOperation.REMOTE_READ
        || mParameters.mOperation == FuseIOOperation.CLUSTER_READ)
        && !mBaseParameters.mDistributed) {
      throw new IllegalArgumentException(String.format(
          "Single-node Fuse IO stress bench doesn't support RemoteRead or ClusterRead."
      ));
    }
    File[] jobWorkerDirs = localPath.listFiles();
    if (jobWorkerDirs == null) {
      throw new IOException(String.format(
          "--local-path %s is not a valid path for this bench. Make sure using the correct path",
              mParameters.mLocalPath
      ));
    }
    if (!mBaseParameters.mDistributed) {
      // single-node case only has one directory
      mJobWorkerDirNames = Arrays.asList(mBaseParameters.mId);
      return;
    }
    // for cluster mode, find 0-based id, and make sure directories and job workers are 1-to-1
    int numJobWorkers;
    try (JobMasterClient client = JobMasterClient.Factory.create(
        JobMasterClientContext.newBuilder(ClientContext.create(new InstancedConfiguration(
            ConfigurationUtils.defaults()))).build())) {
      numJobWorkers = client.getAllWorkerHealth().size();
    }
    if (numJobWorkers != jobWorkerDirs.length) {
      throw new IllegalStateException("Some job worker crashed or joined after data are written. "
          + "The test is stopped.");
    }
    mJobWorkerDirNames = Arrays.asList(jobWorkerDirs).stream()
        .map(file -> file.getName())
        .collect(Collectors.toList());
    mJobWorkerZeroBasedId = mJobWorkerDirNames.indexOf(mBaseParameters.mId);
    if (mJobWorkerZeroBasedId == -1) {
      throw new IllegalStateException(String.format(
          "Directory %s is not found. Please use this bench to generate test files, and make sure "
              + "no job worker crashes or joins after data is written. The test is stopped.",
              mBaseParameters.mId));
    }
  }

  @Override
  public FuseIOTaskResult runLocal() throws Exception {
    FuseIOTaskResult taskResult = runFuseBench();
    taskResult.setBaseParameters(mBaseParameters);
    taskResult.setParameters(mParameters);

    return taskResult;
  }

  private FuseIOTaskResult runFuseBench() throws Exception {
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mThreads).create();
    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (startMs == BaseParameters.UNDEFINED_START_MS || mStartBarrierPassed) {
      // if the barrier was already passed, then overwrite the start time
      startMs = CommonUtils.getCurrentMs() + 10000;
    }
    long endMs = startMs + warmupMs + durationMs;
    BenchContext context = new BenchContext(startMs, endMs);

    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
    for (int i = 0; i < mParameters.mThreads; i++) {
      callables.add(new BenchThread(context, i));
    }
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);

    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);

    FuseIOTaskResult result = context.getResult();

    LOG.info(String.format("job worker id: %s, errors: %d, IO throughput (MB/s): %f",
        mBaseParameters.mId, result.getErrors().size(), result.getIOMBps()));

    return result;
  }

  /**
   * Reads the log file from java agent log file.
   *
   * @param startMs start time for profiling
   * @param endMs end time for profiling
   * @return TimeToFirstByteStatistics
   * @throws IOException
   */
  @SuppressFBWarnings(value = "DMI_HARDCODED_ABSOLUTE_FILENAME")
  public synchronized Map<String, SummaryStatistics> addAdditionalResult(
      long startMs, long endMs) throws IOException {
    Map<String, SummaryStatistics> summaryStatistics = new HashMap<>();

    Map<String, MethodStatistics> nameStatistics =
        processMethodProfiles(startMs, endMs, profileInput -> {
          if (profileInput.getIsttfb()) {
            return profileInput.getMethod();
          }
          return null;
        });
    if (!nameStatistics.isEmpty()) {
      for (Map.Entry<String, MethodStatistics> entry : nameStatistics.entrySet()) {
        summaryStatistics.put(
            entry.getKey(), toSummaryStatistics(entry.getValue()));
      }
    }

    return summaryStatistics;
  }

  /**
   * Converts this class to {@link SummaryStatistics}.
   *
   * @param methodStatistics the method statistics
   * @return new SummaryStatistics
   */
  private SummaryStatistics toSummaryStatistics(MethodStatistics methodStatistics) {
    float[] responseTimePercentile = new float[101];
    for (int i = 0; i <= 100; i++) {
      responseTimePercentile[i] =
          (float) methodStatistics.getTimeNs().getValueAtPercentile(i) / Constants.MS_NANO;
    }

    float[] responseTime99Percentile = new float[StressConstants.TIME_99_COUNT];
    for (int i = 0; i < responseTime99Percentile.length; i++) {
      responseTime99Percentile[i] = (float) methodStatistics.getTimeNs()
          .getValueAtPercentile(100.0 - 1.0 / (Math.pow(10.0, i))) / Constants.MS_NANO;
    }

    float[] maxResponseTimesMs = new float[StressConstants.MAX_TIME_COUNT];
    Arrays.fill(maxResponseTimesMs, -1);
    for (int i = 0; i < methodStatistics.getMaxTimeNs().length; i++) {
      maxResponseTimesMs[i] = (float) methodStatistics.getMaxTimeNs()[i] / Constants.MS_NANO;
    }

    return new SummaryStatistics(methodStatistics.getNumSuccess(),
        responseTimePercentile,
        responseTime99Percentile, maxResponseTimesMs);
  }

  private final class BenchContext {
    private final long mStartMs;
    private final long mEndMs;

    /** The results. Access must be synchronized for thread safety. */
    private FuseIOTaskResult mFuseIOTaskResult;

    public BenchContext(long startMs, long endMs) {
      mStartMs = startMs;
      mEndMs = endMs;
    }

    public long getStartMs() {
      return mStartMs;
    }

    public long getEndMs() {
      return mEndMs;
    }

    public synchronized void mergeThreadResult(FuseIOTaskResult threadResult) {
      if (mFuseIOTaskResult == null) {
        mFuseIOTaskResult = threadResult;
      } else {
        try {
          mFuseIOTaskResult.merge(threadResult);
        } catch (Exception e) {
          mFuseIOTaskResult.addErrorMessage(e.getMessage());
        }
      }
    }

    public synchronized FuseIOTaskResult getResult() {
      return mFuseIOTaskResult;
    }
  }

  private final class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final int mThreadId;
    private final byte[] mBuffer;
    private final long mFileSize;

    private FileInputStream mInStream = null;
    private FileOutputStream mOutStream = null;
    private long mCurrentOffset;
    private long mRecordMs;

    private final FuseIOTaskResult mFuseIOTaskResult = new FuseIOTaskResult();

    private BenchThread(BenchContext context, int threadId) {
      mContext = context;
      mThreadId = threadId;

      mBuffer = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mBufferSize)];
      Arrays.fill(mBuffer, (byte) 'A');

      mFileSize = FormatUtils.parseSpaceSize(mParameters.mFileSize);
      // actual time to start measurement
      mRecordMs = mContext.getStartMs() + FormatUtils.parseTimeSize(mParameters.mWarmup);
    }

    @Override
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
        LOG.error(Thread.currentThread().getName() + ": failed", e);
        mFuseIOTaskResult.addErrorMessage(e.getMessage());
      } finally {
        closeInStream();
        closeOutStream();
      }

      // update bench result by merging in individual thread result
      mFuseIOTaskResult.setEndMs(CommonUtils.getCurrentMs());
      mContext.mergeThreadResult(mFuseIOTaskResult);

      return null;
    }

    private void runInternal() throws Exception {
      mFuseIOTaskResult.setRecordStartMs(mRecordMs);
      long waitMs = mContext.getStartMs() - CommonUtils.getCurrentMs();
      if (waitMs < 0) {
        throw new IllegalStateException(String.format(
            "Thread missed barrier. Increase the start delay. start: %d current: %d",
            mContext.getStartMs(), CommonUtils.getCurrentMs()));
      }
      CommonUtils.sleepMs(waitMs);
      mStartBarrierPassed = true;

      switch (mParameters.mOperation) {
        case LIST_FILE: {
          listFile();
          break;
        }
        case WRITE: // fall through
        case LOCAL_READ: {
          writeOrLocalRead();
          break;
        }
        case REMOTE_READ: // fall through
        case CLUSTER_READ: {
          remoteOrClusterRead();
          break;
        }
        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }

    private void listFile() {
      for (String nameJobWorkerDir : mJobWorkerDirNames) {
        for (int testDirId = mThreadId; testDirId < mParameters.mNumDirs;
             testDirId += mParameters.mThreads) {
          String dirPath = String.format(TEST_DIR_STRING_FORMAT, mParameters.mLocalPath,
                  nameJobWorkerDir, testDirId);
          File dir = new File(dirPath);
          dir.listFiles();
        }
      }
    }

    private void writeOrLocalRead() throws Exception {
      for (int testDirId = mThreadId; testDirId < mParameters.mNumDirs;
          testDirId += mParameters.mThreads) {
        for (int testFileId = 0; testFileId < mParameters.mNumFilesPerDir; testFileId++) {
          String filePath = String.format(TEST_FILE_STRING_FORMAT,
              mParameters.mLocalPath, mBaseParameters.mId, testDirId, testFileId);
          boolean stopTest = processFile(filePath, FuseIOOperation.isRead(mParameters.mOperation));
          if (stopTest) {
            return;
          }
        }
      }
      finishProcessingFiles();
    }

    private void remoteOrClusterRead() throws Exception {
      for (int numJobWorkerDirProcessed = 0; numJobWorkerDirProcessed < mJobWorkerDirNames.size();
          numJobWorkerDirProcessed++) {
        // find which job worker directory to read
        int indexCurrentJobWorkerDir = (numJobWorkerDirProcessed + mJobWorkerZeroBasedId)
            % mJobWorkerDirNames.size();
        // skip itself if the operation is remote read
        if (indexCurrentJobWorkerDir == mJobWorkerZeroBasedId
            && mParameters.mOperation == FuseIOOperation.REMOTE_READ) {
          continue;
        }
        String nameCurrentJobWorkerDir = mJobWorkerDirNames.get(indexCurrentJobWorkerDir);

        // find which files to read under this job worker directory
        for (int testDirId = mJobWorkerZeroBasedId; testDirId < mParameters.mNumDirs;
            testDirId += mJobWorkerDirNames.size()) {
          for (int testFileId = mThreadId; testFileId < mParameters.mNumFilesPerDir;
              testFileId += mParameters.mThreads) {
            String filePath = String.format(TEST_FILE_STRING_FORMAT,
                mParameters.mLocalPath, nameCurrentJobWorkerDir, testDirId, testFileId);
            boolean stopTest = processFile(filePath,
                FuseIOOperation.isRead(mParameters.mOperation));
            if (stopTest) {
              return;
            }
          }
        }
      }
      finishProcessingFiles();
    }

    /**
     * Method for processing a given file.
     *
     * @param filePath the path of the file to process
     * @param isRead whether the operation is read
     * @return whether the test should be stopped because of time or interruption
     */
    private boolean processFile(String filePath, boolean isRead) throws IOException {
      mCurrentOffset = 0;
      while (!Thread.currentThread().isInterrupted()) {
        if (isRead && CommonUtils.getCurrentMs() > mContext.getEndMs()) {
          closeInStream();
          return true;
        }
        long ioBytes = applyOperation(filePath);

        // done reading/writing one file
        if (ioBytes <= 0) {
          return false;
        }
        // start recording after the warmup
        if (CommonUtils.getCurrentMs() > mFuseIOTaskResult.getRecordStartMs()) {
          mFuseIOTaskResult.incrementIOBytes(ioBytes);
        }
      }
      return true;
    }

    private long applyOperation(String filePath) throws IOException {
      if (FuseIOOperation.isRead(mParameters.mOperation) && mInStream == null) {
        mInStream = new FileInputStream(filePath);
      }
      switch (mParameters.mOperation) {
        case LOCAL_READ: // fall through
        case REMOTE_READ: // fall through
        case CLUSTER_READ: {
          if (mInStream == null) {
            mInStream = new FileInputStream(filePath);
          }
          int bytesRead = mInStream.read(mBuffer);
          if (bytesRead < 0) {
            closeInStream();
          }
          return bytesRead;
        }
        case WRITE: {
          if (mOutStream == null) {
            mOutStream = new FileOutputStream(filePath);
          }
          int bytesToWrite = (int) Math.min(mFileSize - mCurrentOffset, mBuffer.length);
          if (bytesToWrite == 0) {
            closeOutStream();
            return -1;
          }
          mOutStream.write(mBuffer, 0, bytesToWrite);
          mCurrentOffset += bytesToWrite;
          return bytesToWrite;
        }
        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }

    private void finishProcessingFiles() {
      if (FuseIOOperation.isRead(mParameters.mOperation)) {
        throw new IllegalArgumentException(String.format("Thread %d finishes reading all its files "
            + "before the bench ends. For more accurate result, use more files, or larger files, "
            + "or a shorter duration", mThreadId));
      }
    }

    private void closeInStream() {
      try {
        if (mInStream != null) {
          mInStream.close();
        }
      } catch (IOException e) {
        mFuseIOTaskResult.addErrorMessage(e.getMessage());
      } finally {
        mInStream = null;
      }
    }

    private void closeOutStream() {
      try {
        if (mOutStream != null) {
          mOutStream.close();
        }
      } catch (IOException e) {
        mFuseIOTaskResult.addErrorMessage(e.getMessage());
      } finally {
        mOutStream = null;
      }
    }
  }
}
