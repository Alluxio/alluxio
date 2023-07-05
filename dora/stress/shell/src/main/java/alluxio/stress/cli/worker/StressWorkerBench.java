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

package alluxio.stress.cli.worker;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.WritePType;
import alluxio.stress.BaseParameters;
import alluxio.stress.cli.AbstractStressBench;
import alluxio.stress.common.FileSystemParameters;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.stress.worker.WorkerBenchTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import static alluxio.stress.BaseParameters.DEFAULT_TASK_ID;

/**
 * Single node stress test.
 */
// TODO(jiacheng): avoid the implicit casts and @SuppressFBWarnings
public class StressWorkerBench extends AbstractStressBench<WorkerBenchTaskResult,
    WorkerBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(StressWorkerBench.class);

  private FileSystem[] mCachedFs;
  private Path[] mFilePaths;
  private Integer[] mOffsets;
  private Integer[] mLengths;

  /** generate random number in range [min, max] (include both min and max).*/
  private Integer randomNumInRange(Random rand, int min, int max) {
    return rand.nextInt(max - min + 1) + min;
  }

  /**
   * Creates instance.
   */
  public StressWorkerBench() {
    mParameters = new WorkerBenchParameters();
  }

  private int getTotalFileNumber() {
    int clusterSize = mBaseParameters.mClusterLimit;
    int threads = mParameters.mThreads;
    int numFiles = clusterSize * threads;
    LOG.info("Total {} * {} = {} files will be generated",
        clusterSize, threads, numFiles);
    return numFiles;
  }

  private Path calculateFilePath(Path base, int workerIdx, int threadIdx) {
    return new Path(base, "worker-" + workerIdx + "-thread-" + threadIdx);
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new StressWorkerBench());
  }

  @Override
  public String getBenchDescription() {
    // TODO(jiacheng): change this description
    return String.join("\n", ImmutableList.of(
        "A benchmarking tool to measure the read performance of alluxio workers in the cluster",
        "The test will create one file and repeatedly read the created file to test the "
            + "performance",
        "",
        "Example:",
        "# This would create a 100MB file with block size of 16KB and then read the file "
            + "for 30s after 10s warmup",
        "$ bin/alluxio runClass alluxio.stress.cli.worker.StressWorkerBench --clients 1 "
            + "--base alluxio:///stress-worker-base --block-size 16k --file-size 100m "
            + "--warmup 10s --duration 30s --cluster\n"
    ));
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public void prepare() throws Exception {
    validateParams();
    Path basePath = new Path(mParameters.mBasePath);
    int fileSize = (int) FormatUtils.parseSpaceSize(mParameters.mFileSize);
    // TODO(jiacheng): Code here will execute on the job workers too, if that;s not desired,
    //  move into the if branch
    // numFiles is the total number of files, to be read by all workers
    // clusterSize is the number of job workers to run test on
    // clients is the number of AlluxioFS instances on each job worker
    // threads is the number of thread per client
    // so total clusterSize * clients * threads read numFiles
    // TODO(jiacheng): Using this here means this param becomes crucial
    int clusterSize = mBaseParameters.mClusterLimit;
    int clients = mParameters.mClients;
    int threads = mParameters.mThreads;
    int numFiles = getTotalFileNumber();
    LOG.info("Total {} * {} * {} = {} files in the test",
        clusterSize, clients, threads, numFiles);

    // Generate the file paths using the same heuristics so all nodes have the same set of paths
    // and offsets
    mFilePaths = new Path[numFiles];
    // set random offsets and lengths if enabled
    mLengths = new Integer[numFiles];
    mOffsets = new Integer[numFiles];

    Random rand = new Random();
    if (mParameters.mIsRandom) {
      rand = new Random(mParameters.mRandomSeed);
    }
    // TODO(jiacheng): get rid of the numFiles param and simply calculate
    for (int i = 0; i < clusterSize; i++) {
      for (int j = 0; j < threads; j++) {
        Path filePath = calculateFilePath(basePath, i, j);
        int index = i * j + j;
        mFilePaths[index] = filePath;
        // TODO(jiacheng): re-check this randomness later
        if (mParameters.mIsRandom) {
          int randomMin = (int) FormatUtils.parseSpaceSize(mParameters.mRandomMinReadLength);
          int randomMax = (int) FormatUtils.parseSpaceSize(mParameters.mRandomMaxReadLength);
          mOffsets[index] = randomNumInRange(rand, 0, fileSize - 1 - randomMin);
          mLengths[index] = randomNumInRange(rand, randomMin,
                  Integer.min(fileSize - mOffsets[i], randomMax));
        } else {
          mOffsets[index] = 0;
          mLengths[index] = fileSize;
        }
      }
    }

    // Generate test files if necessary
    if (mBaseParameters.mDistributed){
      LOG.info("Running in distributed mode on a job worker. The test file should have been "
          + "prepared in the commandline process before distributing the tasks.");
    } else {
      if (mParameters.mSkipCreation) {
        LOG.info("Test file preparation is skipped");
      } else {
        LOG.info("Preparing the test files in the command line");
        // set hdfs conf for preparation client
        Configuration hdfsConf = new Configuration();
        hdfsConf.set(PropertyKey.Name.USER_FILE_DELETE_UNCHECKED, "true");
        hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, mParameters.mWriteType);
        FileSystem prepareFs = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);
        prepareTestFiles(basePath, fileSize, numFiles, prepareFs);
      }
    }

    // Create HDFS config for all FS instances
    // Create those FS instances and cache, for the testing step
    Configuration hdfsConf = new Configuration();
    // do not cache these clients
    hdfsConf.set(
        String.format("fs.%s.impl.disable.cache", (new URI(mParameters.mBasePath)).getScheme()),
        "true");
    for (Map.Entry<String, String> entry : mParameters.mConf.entrySet()) {
      hdfsConf.set(entry.getKey(), entry.getValue());
    }
    LOG.info("HDFS config used in the test: {}", hdfsConf);

    mCachedFs = new FileSystem[mParameters.mClients];
    for (int i = 0; i < mCachedFs.length; i++) {
      mCachedFs[i] = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);
    }
  }

  private void prepareTestFiles(Path basePath, int fileSize, int numFiles, FileSystem prepareFs) throws IOException {
    LOG.info("Preparing {} test files under {}", numFiles, basePath);
    if (prepareFs.exists(basePath)) {
      LOG.info("The base path exists, delete it first.");
      prepareFs.delete(basePath, true);
    }
    // This base path in UFS will be shared by all workers
    LOG.info("Creating the new base path directory");
    prepareFs.mkdirs(basePath);
    LOG.info("Empty base path directory created");
    // TODO(jiacheng): Is this buffer size still applicable?
    byte[] buffer = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mBufferSize)];
    Arrays.fill(buffer, (byte) 'A');

    LOG.info("Creating {} files...", numFiles);
    for (int i = 0; i < numFiles; i++) {
      if (i > 0 && i % 1000 == 0) {
        LOG.info("{} files created", i);
      }
      Path filePath = mFilePaths[i];
      try (FSDataOutputStream mOutStream = prepareFs
              // TODO(jiacheng): this is using the block size, should use page size?
          .create(filePath, false, buffer.length, (short) 1,
              FormatUtils.parseSpaceSize(mParameters.mBlockSize))) {
        while (true) {
          int bytesToWrite = (int) Math.min(fileSize - mOutStream.getPos(), buffer.length);
          if (bytesToWrite == 0) {
            break;
          }
          mOutStream.write(buffer, 0, bytesToWrite);
        }
      }
    }
    LOG.info("All test files created");
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public WorkerBenchTaskResult runLocal() throws Exception {
    // TODO(jiacheng): Based on the worker ID, can it deduce the range of files to handle?
    LOG.info("Worker ID is {}, index is {}", mBaseParameters.mId, mBaseParameters.mIndex);
    LOG.info("Total {} workers in the cluster", mBaseParameters.mClusterLimit);
    // If running in this one process, do all the work
    // Otherwise, calculate its own part and only do that
    int startFileIndex = 0;
    int endFileIndex = getTotalFileNumber();
    if (mBaseParameters.mIndex.equals(DEFAULT_TASK_ID)) {
      LOG.info("This is running in the command line process. Read all {} files with {} threads.",
          endFileIndex, mParameters.mThreads);
    } else {
      LOG.info("This job worker has index {} among {} workers",
          mBaseParameters.mIndex, mBaseParameters.mClusterLimit);
      int threadNum = mParameters.mThreads;
      int workerIndex = Integer.parseInt(mBaseParameters.mIndex);
      startFileIndex = workerIndex * threadNum;
      endFileIndex = startFileIndex + threadNum;
      LOG.info("This job worker threads read files [{}, {})", startFileIndex, endFileIndex);
    }

    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mThreads).create();

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (mBaseParameters.mStartMs == BaseParameters.UNDEFINED_START_MS) {
      startMs = CommonUtils.getCurrentMs() + 5000;
    }
    long endMs = startMs + warmupMs + durationMs;
    BenchContext context = new BenchContext(startMs, endMs);

    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
    // Each thread keeps reading one same file over and over
    // Each thread will have one file created for it
    for (int threadIndex = 0; threadIndex < mParameters.mThreads; threadIndex++) {
      int fileIndex = startFileIndex + threadIndex;
      LOG.info("Thread {} reads file {} path {}", threadIndex, fileIndex, mFilePaths[fileIndex]);
      callables.add(new BenchThread(context, fileIndex,
          mCachedFs[threadIndex % mCachedFs.length]));
    }
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);

    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);

    // TODO(jiacheng): If there's extra output on the job workers, json deser will be messed up
    return context.getResult();
  }

  @Override
  public void validateParams() throws Exception {
    if (mParameters.mFree && WritePType.MUST_CACHE.name().equals(mParameters.mWriteType)) {
      throw new IllegalStateException(String.format("%s cannot be %s when %s option provided",
              FileSystemParameters.WRITE_TYPE_OPTION_NAME, WritePType.MUST_CACHE, "--free"));
    }
  }

  private static final class BenchContext {
    private final long mStartMs;
    private final long mEndMs;

    /** The results. Access must be synchronized for thread safety. */
    private WorkerBenchTaskResult mResult;

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

    public synchronized void mergeThreadResult(WorkerBenchTaskResult threadResult) {
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

    synchronized WorkerBenchTaskResult getResult() {
      return mResult;
    }
  }

  private final class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final int mTargetFileIndex;
    private final FileSystem mFs;
    private final byte[] mBuffer;
    private final WorkerBenchTaskResult mResult;
    private final boolean mIsRandomReed;

    private final FSDataInputStream[] mInStreams = new FSDataInputStream[mFilePaths.length];

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private BenchThread(BenchContext context, int targetFileIndex, FileSystem fs) {
      mContext = context;
      mTargetFileIndex = targetFileIndex;
      mFs = fs;
      mBuffer = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mBufferSize)];

      mResult = new WorkerBenchTaskResult();
      mResult.setParameters(mParameters);
      mResult.setBaseParameters(mBaseParameters);
      mIsRandomReed = mParameters.mIsRandom;
    }

    @Override
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
        LOG.error(Thread.currentThread().getName() + ": failed", e);
        mResult.addErrorMessage(e.getMessage());
      } finally {
        for (int i = 0; i < mInStreams.length; i++) {
          closeInStream(i);
        }
      }

      // Update local thread end time
      mResult.setEndMs(CommonUtils.getCurrentMs());

      // merge local thread result with full result
      mContext.mergeThreadResult(mResult);
      return null;
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runInternal() throws Exception {
      // When to start recording measurements
      long recordMs = mContext.getStartMs() + FormatUtils.parseTimeSize(mParameters.mWarmup);
      mResult.setRecordStartMs(recordMs);

      long waitMs = mContext.getStartMs() - CommonUtils.getCurrentMs();
      if (waitMs < 0) {
        throw new IllegalStateException(String.format(
            "Thread missed barrier. Increase the start delay. start: %d current: %d",
            mContext.getStartMs(), CommonUtils.getCurrentMs()));
      }
      CommonUtils.sleepMs(waitMs);

      while (!Thread.currentThread().isInterrupted()
          && CommonUtils.getCurrentMs() < mContext.getEndMs()) {
        // Keep reading the same file
        int ioBytes = applyOperation(mTargetFileIndex);
        long currentMs = CommonUtils.getCurrentMs();
        // Start recording after the warmup
        if (currentMs > recordMs) {
          if (ioBytes > 0) {
            mResult.incrementIOBytes(ioBytes);
          }
        }
      }
    }

    /**
     * Read the file by the offset and length based on the given index.
     * @param i the index of the path, offset and length of the target file
     * @return the actual red byte number
     */
    private int applyOperation(int i) throws IOException {
      Path filePath = mFilePaths[i];
      int offset = mOffsets[i];
      int length = mLengths[i];

      if (mInStreams[i] == null) {
        mInStreams[i] = mFs.open(filePath);
      }

      int bytesRead = 0;
      if (mIsRandomReed) {
        while (length > 0) {
          int actualReadLength = mInStreams[i]
              .read(offset, mBuffer, 0, mBuffer.length);
          if (actualReadLength < 0) {
            closeInStream(i);
            break;
          } else {
            bytesRead += actualReadLength;
            length -= actualReadLength;
            offset += actualReadLength;
          }
        }
      } else {
        while (true) {
          int actualReadLength = mInStreams[i].read(mBuffer);
          if (actualReadLength < 0) {
            closeInStream(i);
            mInStreams[i] = mFs.open(filePath);
            break;
          } else {
            bytesRead += actualReadLength;
          }
        }
      }
      return bytesRead;
    }

    private void closeInStream(int i) {
      try {
        if (mInStreams[i] != null) {
          mInStreams[i].close();
        }
      } catch (IOException e) {
        mResult.addErrorMessage(e.getMessage());
      } finally {
        mInStreams[i] = null;
      }
    }
  }
}
