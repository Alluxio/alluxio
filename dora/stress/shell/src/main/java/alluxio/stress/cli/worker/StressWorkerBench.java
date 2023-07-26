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

import static alluxio.stress.BaseParameters.DEFAULT_TASK_ID;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.dora.WorkerLocationPolicy;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.WritePType;
import alluxio.hadoop.HadoopUtils;
import alluxio.stress.BaseParameters;
import alluxio.stress.cli.AbstractStressBench;
import alluxio.stress.common.FileSystemParameters;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.stress.worker.WorkerBenchTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockWorker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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

/**
 * Single node stress test.
 */
// TODO(jiacheng): avoid the implicit casts and @SuppressFBWarnings
public class StressWorkerBench extends AbstractStressBench<WorkerBenchTaskResult,
    WorkerBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(StressWorkerBench.class);
  private static final long DUMMY_BLOCK_SIZE = 64 * Constants.MB;

  private FileSystem[] mCachedFs;
  private Path[] mFilePaths;
  private Integer[] mOffsets;
  private Integer[] mLengths;
  private FileSystemContext mFsContext;
  private DoraCacheFileSystem mDoraFs;

  /** generate random number in range [min, max] (include both min and max).*/
  private Integer randomNumInRange(Random rand, int min, int max) {
    return rand.nextInt(max - min + 1) + min;
  }

  /**
   * Creates instance.
   */
  public StressWorkerBench() {
    mParameters = new WorkerBenchParameters();
    InstancedConfiguration conf =
        new InstancedConfiguration(alluxio.conf.Configuration.global().copyProperties());
    conf.set(PropertyKey.DORA_ENABLED, true);
    mFsContext = FileSystemContext.create(conf);
    alluxio.client.file.FileSystem testFs = alluxio.client.file.FileSystem.Factory.create(mFsContext);
    mDoraFs = (DoraCacheFileSystem) testFs;
  }

  /***
   * mClusterLimit is the number of job workers to run test on, if running in local mode then
   * this equals 1.
   * mThreads is the number of thread per job worker.
   * We allocate one file for each thread, so total mClusterLimit * mThreads files.
   */
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
    // numFiles is the total number of files, to be read by all workers
    // clusterSize is the number of job workers to run test on
    // clients is the number of AlluxioFS instances on each job worker
    // threads is the number of thread per client
    // so total clusterSize * threads read numFiles
    int clusterSize = mBaseParameters.mClusterLimit;
    int threads = mParameters.mThreads;
    int numFiles = getTotalFileNumber();

    // Generate the file paths using the same heuristics so all nodes have the same set of paths
    // and offsets
    mFilePaths = new Path[numFiles];
    // set random offsets and lengths if enabled
    mLengths = new Integer[numFiles];
    mOffsets = new Integer[numFiles];

    generateTestFilePaths(basePath);

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
        prepareTestFiles(basePath, fileSize, prepareFs);
      }
    }

    // Create HDFS config for all FS instances
    // Create those FS instances and cache, for the testing step
    Configuration hdfsConf = new Configuration();
    // do not cache these clients
    hdfsConf.set(
        String.format("fs.%s.impl.disable.cache", (new URI(mParameters.mBasePath)).getScheme()),
        "true");
    // TODO(jiacheng): we may need a policy to only IO to remote worker
    hdfsConf.set(PropertyKey.Name.USER_WORKER_SELECTION_POLICY,
        "alluxio.client.file.dora.LocalWorkerPolicy");
    for (Map.Entry<String, String> entry : mParameters.mConf.entrySet()) {
      hdfsConf.set(entry.getKey(), entry.getValue());
    }
    LOG.info("HDFS config used in the test: {}", hdfsConf);

    mCachedFs = new FileSystem[mParameters.mClients];
    for (int i = 0; i < mCachedFs.length; i++) {
      mCachedFs[i] = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);
    }
  }

  @VisibleForTesting
  public void generateTestFilePaths(Path basePath) throws IOException {
    int fileSize = (int) FormatUtils.parseSpaceSize(mParameters.mFileSize);
    int clusterSize = mBaseParameters.mClusterLimit;
    int threads = mParameters.mThreads;
    // We assume the worker list does not change at this stage of test
    List<BlockWorkerInfo> workers = mFsContext.getCachedWorkers();
    LOG.info("Available workers in the cluster are {}", workers);

    Random rand = new Random();
    if (mParameters.mIsRandom) {
      rand = new Random(mParameters.mRandomSeed);
    }

    // TODO(jiacheng): by adding LocalWorkerPolicy, we no longer need these
    // This distribution keeps track of how many connections each worker will serve
//    int[] distribution = new int[clusterSize];
//    boolean isLocal = false;
//    if (mParameters.mMode.equals("LOCAL")){
//      LOG.info("Running test in LOCAL mode, meaning all clients should read local worker");
//      isLocal = true;
//    } else if (mParameters.mMode.equals("REMOTE")) {
//      LOG.info("Running test in REMOTE mode, meaning all clients must not read local workers");
//    } else {
//      throw new IllegalArgumentException("Unrecognized mode " + mParameters.mMode);
//    }

    for (int i = 0; i < clusterSize; i++) {
      BlockWorkerInfo localWorker = workers.get(i);
      LOG.info("Building file paths for worker {}", localWorker);
      for (int j = 0; j < threads; j++) {
        Path filePath = calculateFilePath(basePath, i, j);
        // TODO(jiacheng): by adding LocalWorkerPolicy, we no longer need this hash check
//        Path filePathAfterSalt = forecastLocation(filePathBeforeSalt, threads, isLocal, distribution, i, workers, localWorker);

        int index = i * threads + j;
        mFilePaths[index] = filePath;

        // Continue init other aspects of the file read operation
        // TODO(jiacheng): do we want a new randomness for every read?
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
    LOG.info("{} file paths generated", mFilePaths.length);
  }

  private Path forecastLocation(Path filePathBeforeSalt, int threadNumber, boolean isLocal,
                                int[] distribution, int currentWorkerIdx,
                                List<BlockWorkerInfo> workers, BlockWorkerInfo localWorker) {
    boolean isFound = false;
    Path filePathAfterSalt = null;
    int salt = 0;
    for (; salt < 1024; salt++) {
      // TODO(jiacheng): what happens if this does not stop
      filePathAfterSalt = filePathBeforeSalt.suffix("-" + salt);

      // Check if the salt can meet our need
      WorkerNetAddress targetWorker = calculateAllocation(filePathAfterSalt, mDoraFs, workers);

      if (isLocal) {
        // accept only if the target worker is local to the client
        // otherwise reject
        if (targetWorker.equals(localWorker.getNetAddress())) {
          isFound = true;
          distribution[currentWorkerIdx]++;
          break; // this salt will give the client a local worker in the hash ring
        }
      } else {
        // accept if:
        // 1. the target worker is not local to the client AND
        if (targetWorker.equals(localWorker.getNetAddress())) {
          // Reject the allocation to a local worker because we want remote
          continue;
        }
        // 2. distribution is even among the workers
        //    on each worker we create #threads requests so each worker should serve that many
        int allocationIndex = findWorkerByAddress(workers, targetWorker);
        if (allocationIndex == -1) {
          throw new IllegalStateException(String.format(
                  "Failed to find worker with address %s in %s", targetWorker, workers));
        }
        if (distribution[allocationIndex] < threadNumber) {
          isFound = true;
          distribution[allocationIndex]++;
          break;
        }
      }
    }
    LOG.info("Tried {} salts for file {}", salt, filePathBeforeSalt);

    // If we exhausted all salts and no fitting path has been found, give up
    if (!isFound) {
      if (isLocal) {
        throw new IllegalStateException(String.format("exhausted %s salts but still cannot find a local worker for path %s",
                1024, filePathBeforeSalt));
      } else {
        throw new IllegalStateException(String.format("exhausted %s salts but still cannot find a remote worker for path %s",
                1024, filePathBeforeSalt));
      }
    }
    return filePathAfterSalt;
  }

  private WorkerNetAddress calculateAllocation(Path proposedPath,
                                               DoraCacheFileSystem doraFs,
                                               List<BlockWorkerInfo> availableWorkers) {
    // This copies from how a hadoop Path translates to a target worker in Dora
    // However this method is extremely fragile to code changes in Dora
    AlluxioURI alluxioUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(proposedPath));
    AlluxioURI ufsUri = doraFs.convertAlluxioPathToUFSPath(alluxioUri);
    String pathToHash = ufsUri.toString();
    return doraFs.getClient().getWorkerNetAddress(pathToHash);
  }

  private int findWorkerByAddress(List<BlockWorkerInfo> workers, WorkerNetAddress targetWorkerAddr) {
    for (int i = 0; i < workers.size(); i++) {
      if (workers.get(i).getNetAddress().equals(targetWorkerAddr)) {
        return i;
      }
    }
    return -1;
  }

  private void prepareTestFiles(Path basePath, int fileSize, FileSystem prepareFs) throws IOException {
    int numFiles = mFilePaths.length;
    LOG.info("Preparing {} test files under {}", numFiles, basePath);
    if (prepareFs.exists(basePath)) {
      LOG.info("The base path exists, delete it first.");
      prepareFs.delete(basePath, true);
    }
    // This base path in UFS will be shared by all workers
    LOG.info("Creating the new base path directory");
    prepareFs.mkdirs(basePath);
    LOG.info("Empty base path directory created");

    byte[] buffer = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mBufferSize)];
    Arrays.fill(buffer, (byte) 'A');

    LOG.info("Creating {} files...", numFiles);
    for (int i = 0; i < numFiles; i++) {
      if (i > 0 && i % 1000 == 0) {
        LOG.info("{} files created", i);
      }
      Path filePath = mFilePaths[i];
      LOG.info("Creating file {}", filePath);
      try (FSDataOutputStream mOutStream = prepareFs
          .create(filePath, false, buffer.length, (short) 1, DUMMY_BLOCK_SIZE)) {
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
    } else {
      startMs = CommonUtils.getCurrentMs() + startMs;
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
    return context.getResult();
  }

  @Override
  public void validateParams() throws Exception {
    if (mBaseParameters.mClusterLimit <= 0) {
      throw new IllegalStateException("--cluster-limit cannot be " + mBaseParameters.mClusterLimit
          + " in StressWorkerBench. It should be a positive number. Use 1 if running in local mode");
    }
    if (mParameters.mThreads <= 0) {
      throw new IllegalStateException("Thread number cannot be " + mParameters.mThreads
          + " in StressWorkerBench. It should be a positive number.");
    }
    if (mParameters.mFree && WritePType.MUST_CACHE.name().equals(mParameters.mWriteType)) {
      throw new IllegalStateException(String.format("%s cannot be %s when %s option provided",
              FileSystemParameters.WRITE_TYPE_OPTION_NAME, WritePType.MUST_CACHE, "--free"));
    }
  }

  @VisibleForTesting
  public Path[] getFilePaths() {
    return mFilePaths;
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
    private final boolean mIsRandomRead;

    private FSDataInputStream mInStream;

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private BenchThread(BenchContext context, int targetFileIndex, FileSystem fs) {
      mContext = context;
      mTargetFileIndex = targetFileIndex;
      mFs = fs;
      mBuffer = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mBufferSize)];

      mResult = new WorkerBenchTaskResult();
      mResult.setParameters(mParameters);
      mResult.setBaseParameters(mBaseParameters);
      mIsRandomRead = mParameters.mIsRandom;
    }

    @Override
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
        LOG.error(Thread.currentThread().getName() + ": failed", e);
        mResult.addErrorMessage(e.getMessage());
      } finally {
        closeInStream();
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
        int ioBytes = applyOperation();
        long currentMs = CommonUtils.getCurrentMs();
        // Start recording after the warmup
        if (currentMs > recordMs) {
          if (ioBytes > 0) {
            mResult.incrementIOBytes(ioBytes);
          } else {
            LOG.warn("Thread for file {} read 0 bytes from I/O", mFilePaths[mTargetFileIndex]);
          }
        }
      }
    }

    /**
     * Read the file by the offset and length based on the given index.
     * @return the actual red byte number
     */
    private int applyOperation() throws IOException {
      Path filePath = mFilePaths[mTargetFileIndex];
      int offset = mOffsets[mTargetFileIndex];
      int length = mLengths[mTargetFileIndex];

      if (mInStream == null) {
        mInStream = mFs.open(filePath);
      }

      int bytesRead = 0;
      if (mIsRandomRead) {
        while (length > 0) {
          int actualReadLength = mInStream
              .read(offset, mBuffer, 0, mBuffer.length);
          if (actualReadLength < 0) {
            closeInStream();
            break;
          } else {
            bytesRead += actualReadLength;
            length -= actualReadLength;
            offset += actualReadLength;
          }
        }
        closeInStream();
      } else {
        while (true) {
          int actualReadLength = mInStream.read(mBuffer);
          if (actualReadLength < 0) {
            closeInStream();
            mInStream = mFs.open(filePath);
            break;
          } else {
            bytesRead += actualReadLength;
          }
        }
      }
      return bytesRead;
    }

    private void closeInStream() {
      try {
        if (mInStream != null) {
          mInStream.close();
        }
      } catch (IOException e) {
        mResult.addErrorMessage(e.getMessage());
      } finally {
        mInStream = null;
      }
    }
  }
}
