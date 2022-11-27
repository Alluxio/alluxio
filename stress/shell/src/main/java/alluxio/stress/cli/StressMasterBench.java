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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
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
import alluxio.util.ShellUtils;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

  protected String extractHostName(String mId) {
    String hostName = "";
    String[] splited_mid = mId.split("-");
    hostName += splited_mid[0];
    for (int i = 1; i < splited_mid.length-1; i++) {
      hostName += "-";
      hostName += splited_mid[i];
    }
    return hostName;
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public void prepare() throws Exception {
    if (mParameters.mFixedCount <= 0) {
      throw new IllegalStateException(
          "fixed count must be > 0. fixedCount: " + mParameters.mFixedCount);
    }

    if (!mBaseParameters.mDistributed) {
      // set hdfs conf for preparation client
      Configuration hdfsConf = new Configuration();
      // force delete, create dirs through to UFS
      hdfsConf.set(PropertyKey.Name.USER_FILE_DELETE_UNCHECKED, "true");
      hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, mParameters.mWriteType);
      // more threads for parallel deletes for cleanup
      hdfsConf.set(PropertyKey.Name.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX, "256");
      FileSystem prepareFs = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);

      // initialize the base, for only the non-distributed task (the cluster launching task)
      Path path = new Path(mParameters.mBasePath);

      // the base path depends on the operation
      Path basePath;
      if (mParameters.mOperation == Operation.CREATE_DIR) {
        basePath = new Path(path, mDirsDir);
      } else if (mParameters.mOperation == Operation.CREATE_TREE || mParameters.mOperation == Operation.LOAD_METADATA) {
        // tbh in CREATE_TREE and LOAD_METADATA basePath was not used
        // here need extract host name from mId since worker ID in random number, need fixed hostname as path
        basePath = new Path(path, extractHostName(mBaseParameters.mId));
      }else {
        basePath = new Path(path, mFilesDir);
      }

      if (mParameters.mOperation == Operation.CREATE_FILE
          || mParameters.mOperation == Operation.CREATE_DIR) {
        LOG.info("Cleaning base path: {}", basePath);
        long start = CommonUtils.getCurrentMs();
        deletePaths(prepareFs, basePath);
        long end = CommonUtils.getCurrentMs();
        LOG.info("Cleanup took: {} s", (end - start) / 1000.0);
        prepareFs.mkdirs(basePath);
      } else if (mParameters.mOperation == Operation.CREATE_TREE || mParameters.mOperation == Operation.LOAD_METADATA) {
        // Do nothing
      } else {
        // these are read operations. the directory must exist
        if (!prepareFs.exists(basePath)) {
          throw new IllegalStateException(String
              .format("base path (%s) must exist for operation (%s)", basePath,
                  mParameters.mOperation));
        }
      }
      if (mParameters.mOperation != Operation.CREATE_TREE && mParameters.mOperation != Operation.LOAD_METADATA) {
        if (!prepareFs.getFileStatus(basePath).isDirectory()) {
          throw new IllegalStateException(String
                  .format("base path (%s) must be a directory for operation (%s)", basePath,
                          mParameters.mOperation));
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

    LOG.info("Using {} to perform the test.", mParameters.mClientType);
    if (mParameters.mClientType == FileSystemClientType.ALLUXIO_HDFS) {
      mCachedFs = new FileSystem[mParameters.mClients];
      for (int i = 0; i < mCachedFs.length; i++) {
        mCachedFs[i] = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);
      }
    } else if (mParameters.mClientType == FileSystemClientType.ALLUXIO_NATIVE) {
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

    // Determine the fixed portion size. Each sub directory has a fixed portion.
    int fixedSize = fs.listStatus(new Path(subDirs[0].getPath(), "fixed")).length;

    long batchSize = 50_000;
    int deleteThreads = 256;
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-delete-thread", deleteThreads).create();

    for (FileStatus subDir : subDirs) {
      LOG.info("Cleaning up all files in: {}", subDir.getPath());
      AtomicLong globalCounter = new AtomicLong();
      Path fixedBase = new Path(subDir.getPath(), "fixed");
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
              Path deletePath;
              if (counter < fixedSize) {
                deletePath = new Path(fixedBase, Long.toString(counter));
              } else {
                deletePath = new Path(subDir.getPath(), Long.toString(counter));
              }
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

    RateLimiter rateLimiter = RateLimiter.create(mParameters.mTargetThroughput);

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
    BenchContext context = new BenchContext(rateLimiter, startMs, endMs);

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
    switch (mParameters.mClientType) {
      case ALLUXIO_HDFS:
        return new AlluxioHDFSBenchThread(context, mCachedFs[index % mCachedFs.length]);
      case ALLUXIO_POSIX:
        return new AlluxioFuseBenchThread(context);
      default:
        return new AlluxioNativeBenchThread(context,
            mCachedNativeFs[index % mCachedNativeFs.length]);
    }
  }

  private final class BenchContext {
    private final RateLimiter mRateLimiter;
    private final long mStartMs;
    private final long mEndMs;
    private final AtomicLong mCounter;
    private final Path mBasePath;
    private final Path mFixedBasePath;

    /** The results. Access must be synchronized for thread safety. */
    private MasterBenchTaskResult mResult;

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    public BenchContext(RateLimiter rateLimiter, long startMs, long endMs) {
      mRateLimiter = rateLimiter;
      mStartMs = startMs;
      mEndMs = endMs;
      mCounter = new AtomicLong();
      if (mParameters.mOperation == Operation.CREATE_DIR) {
        mBasePath =
            new Path(PathUtils.concatPath(mParameters.mBasePath, mDirsDir, mBaseParameters.mId));
      } else if (mParameters.mOperation == Operation.CREATE_TREE || mParameters.mOperation == Operation.LOAD_METADATA) {
        /*
         mId is composed by worker hostname and workerId, and workerId is a random number change when master rebooted.
         thus extract hostname as permanent path which can survive as master formated
        */
        mBasePath = new Path(PathUtils.concatPath(mParameters.mBasePath, extractHostName(mBaseParameters.mId)));
      } else {
        mBasePath =
            new Path(PathUtils.concatPath(mParameters.mBasePath, mFilesDir, mBaseParameters.mId));
      }
      mFixedBasePath = new Path(mBasePath, mFixedDir);
      LOG.info("BenchContext: basePath: {}, fixedBasePath: {}", mBasePath, mFixedBasePath);
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

    public AtomicLong getCounter() {
      return mCounter;
    }

    public Path getBasePath() {
      return mBasePath;
    }

    public Path getFixedBasePath() {
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
    private final Histogram mResponseTimeNs;
    protected final Path mBasePath;
    protected final Path mFixedBasePath;

    // vars for createTestTree
    protected int[] mPathRecord;
    protected int[] mTreeLevelQuant;
    protected int mTreeTotalCount;
    private final MasterBenchTaskResult mResult = new MasterBenchTaskResult();

    private BenchThread(BenchContext context) {
      mContext = context;
      mResponseTimeNs = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
          StressConstants.TIME_HISTOGRAM_PRECISION);
      mBasePath = mContext.getBasePath();
      mFixedBasePath = mContext.getFixedBasePath();

      // prepare createTree parameter
      /* PathRecord[] can be used to generate file path
       * it's length equals to mTreeDepth + 1 since the file(mTreeFiles) level
       * was not included in the mTreeDepth
       */
      // mPathRecord = new int[mParameters.mTreeDepth + 1];
      mPathRecord = new int[mParameters.mTreeDepth];
      /*
        mTreeLevelQuant record total file number for each level in the file tree
       */
      // mTreeLevelQuant = new int[mParameters.mTreeDepth + 1];
      mTreeLevelQuant = new int[mParameters.mTreeDepth];

      // init tree related var
      // mTreeLevelQuant[mParameters.mTreeDepth] = mParameters.mTreeFiles;
      mTreeLevelQuant[mParameters.mTreeDepth - 1] = mParameters.mTreeWidth;
      for (int i = mTreeLevelQuant.length - 2; i >= 0; i--) {
        mTreeLevelQuant[i] = mTreeLevelQuant[i+1] * mParameters.mTreeWidth;
      }
      // Total number of files in the tree, equals to thread count plus file number in single thread tree
      mTreeTotalCount = mTreeLevelQuant[0] * mParameters.mTreeThreads;
      // for (int i = 0; i < mParameters.mTreeDepth; i++) {
      //   System.out.println(mTreeLevelQuant[i]);
      //   System.out.println(mPathRecord[i]);
      // }
    }

    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
        LOG.warn("Exception during bench thread runInternal", e);
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

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private void runInternal() throws IOException, AlluxioException {
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

      long localCounter;

      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        if (mParameters.mOperation != Operation.LOAD_METADATA && mParameters.mOperation != Operation.CREATE_TREE && !useStopCount && CommonUtils.getCurrentMs() >= mContext.getEndMs()) {
          break;
        }
        localCounter = mContext.getCounter().getAndIncrement();

        // stop count for createTree operation
        // for me one problem of stressMasterBench is one parameter has different function in different context, need better insulate
        // thus here is trying to disable useStopCount and use generated mTreeCount instead
        // if you use createTree mStopCount should be -1
        if (mParameters.mOperation == Operation.CREATE_TREE && localCounter >= mTreeTotalCount) {
          break;
        }

        if (mParameters.mOperation == Operation.LOAD_METADATA && localCounter >= mParameters.mThreads) {
          break;
        }

        if (useStopCount && localCounter >= mParameters.mStopCount) {
          break;
        }

        mContext.getRateLimiter().acquire();
        long startNs = System.nanoTime();
        applyOperation(localCounter);
        long endNs = System.nanoTime();

        long currentMs = CommonUtils.getCurrentMs();
        // Start recording after the warmup
        if (currentMs > recordMs) {
          mResult.incrementNumSuccess(1);

          // record response times
          long responseTimeNs = endNs - startNs;
          mResponseTimeNs.recordValue(responseTimeNs);

          // track max response time
          long[] maxResponseTimeNs = mResult.getStatistics().mMaxResponseTimeNs;
          // if you use createTree StopCount should be -1
          int bucket =
              Math.min(maxResponseTimeNs.length - 1, (int) ((currentMs - recordMs) / bucketSize));
          if (responseTimeNs > maxResponseTimeNs[bucket]) {
            maxResponseTimeNs[bucket] = responseTimeNs;
          }
        }
      }
    }

    protected abstract void applyOperation(long counter) throws IOException, AlluxioException;
  }

  private final class AlluxioHDFSBenchThread extends BenchThread {
    private final FileSystem mFs;

    private AlluxioHDFSBenchThread(BenchContext context, FileSystem fs) {
      super(context);
      mFs = fs;
    }

    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    protected void applyOperation(long counter) throws IOException {
      Path path;
      switch (mParameters.mOperation) {
        case CREATE_DIR:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
          }
          mFs.mkdirs(path);
          break;
        case CREATE_FILE:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
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
          counter = counter % mParameters.mFixedCount;
          path = new Path(mFixedBasePath, Long.toString(counter));
          mFs.getFileBlockLocations(path, 0, 0);
          break;
        case GET_FILE_STATUS:
          counter = counter % mParameters.mFixedCount;
          path = new Path(mFixedBasePath, Long.toString(counter));
          mFs.getFileStatus(path);
          break;
        case LIST_DIR:
          FileStatus[] files = mFs.listStatus(mFixedBasePath);
          if (files.length != mParameters.mFixedCount) {
            throw new IOException(String
                .format("listing `%s` expected %d files but got %d files", mFixedBasePath,
                    mParameters.mFixedCount, files.length));
          }
          break;
        case LIST_DIR_LOCATED:
          RemoteIterator<LocatedFileStatus> it = mFs.listLocatedStatus(mFixedBasePath);
          int listedFiles = 0;
          while (it.hasNext()) {
            it.next();
            listedFiles++;
          }
          if (listedFiles != mParameters.mFixedCount) {
            throw new IOException(String
                .format("listing located `%s` expected %d files but got %d files", mFixedBasePath,
                    mParameters.mFixedCount, listedFiles));
          }
          break;
        case OPEN_FILE:
          counter = counter % mParameters.mFixedCount;
          path = new Path(mFixedBasePath, Long.toString(counter));
          mFs.open(path).close();
          break;
        case RENAME_FILE:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
          }
          Path dst = new Path(path + "-renamed");
          if (!mFs.rename(path, dst)) {
            throw new IOException(String.format("Failed to rename (%s) to (%s)", path, dst));
          }
          break;
        case DELETE_FILE:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
          }
          if (!mFs.delete(path, false)) {
            throw new IOException(String.format("Failed to delete (%s)", path));
          }
          break;
        case LOAD_METADATA:
          // mFs.loadmetadata();
          files = mFs.listStatus(mFixedBasePath);
          if (files.length != mParameters.mFixedCount) {
            throw new IOException(String
                    .format("listing `%s` expected %d files but got %d files", mFixedBasePath,
                            mParameters.mFixedCount, files.length));
          }
          break;
        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
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
    protected void applyOperation(long counter) throws IOException, AlluxioException {
      Path path;
      switch (mParameters.mOperation) {
        case CREATE_DIR:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
          }

          mFs.createDirectory(new AlluxioURI(path.toString()),
              CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
          break;
        case CREATE_FILE:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
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
          counter = counter % mParameters.mFixedCount;
          path = new Path(mFixedBasePath, Long.toString(counter));
          mFs.getBlockLocations(new AlluxioURI(path.toString()));
          break;
        case GET_FILE_STATUS:
          counter = counter % mParameters.mFixedCount;
          path = new Path(mFixedBasePath, Long.toString(counter));
          mFs.getStatus(new AlluxioURI(path.toString()));
          break;
        case LIST_DIR:
          List<alluxio.client.file.URIStatus> files
              = mFs.listStatus(new AlluxioURI(mFixedBasePath.toString()));
          if (files.size() != mParameters.mFixedCount) {
            throw new IOException(String
                .format("listing `%s` expected %d files but got %d files", mFixedBasePath,
                    mParameters.mFixedCount, files.size()));
          }
          break;
        case LIST_DIR_LOCATED:
          throw new UnsupportedOperationException("LIST_DIR_LOCATED is not supported!");
        case OPEN_FILE:
          counter = counter % mParameters.mFixedCount;
          path = new Path(mFixedBasePath, Long.toString(counter));
          mFs.openFile(new AlluxioURI(path.toString())).close();
          break;
        case RENAME_FILE:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
          }
          Path dst = new Path(path + "-renamed");
          mFs.rename(new AlluxioURI(path.toString()), new AlluxioURI(dst.toString()));
          break;
        case DELETE_FILE:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
          }

          mFs.delete(new AlluxioURI(path.toString()),
              DeletePOptions.newBuilder().setRecursive(false).build());
          break;
        case LOAD_METADATA:
          // ShellUtils.execCommand("/bin/alluxio LoadMetadata");
          mFs.loadMetadata(new AlluxioURI(mBasePath + "/" + counter), ListStatusPOptions.newBuilder()
                  .setLoadMetadataType(LoadMetadataPType.ALWAYS)
                  .setRecursive(true)
                  .setLoadMetadataOnly(true).build());
          // List<alluxio.client.file.URIStatus> files
          //         = mFs.listStatus(new AlluxioURI(mFixedBasePath.toString()));
          // if (files.size() != mParameters.mFixedCount) {
          //   throw new IOException(String
          //           .format("listing `%s` expected %d files but got %d files", mFixedBasePath,
          //                   mParameters.mFixedCount, files.size()));
          // }
          break;
        case CREATE_TREE:
          String p = "";
          // record value for following path
          int redundent = (int)counter;
          // the thread that file exist
          // for (int i = 0; i < mParameters.mTreeDepth + 1; i++) {
          for (int i = 0; i < mParameters.mTreeDepth; i++) {
            mPathRecord[i] = redundent / mTreeLevelQuant[i];
            redundent = redundent % mTreeLevelQuant[i];
            p += "/";
            p += mPathRecord[i];
          }

          // int mFileIndex = redundent % mParameters.mTreeFiles;
          // here create tree
          // System.out.println(mBasePath + p + "/" + redundent + ".txt");
          // mFs.createFile(new AlluxioURI((mBasePath + p + "/" + redundent + ".txt").toString()), CreateFilePOptions.newBuilder().setRecursive(true).build()).close();
          for (int i = 0; i < mParameters.mTreeFiles; i++) {
            // System.out.println(mBasePath + p + "/" + redundent + "/" + i + ".txt");
            mFs.createFile(new AlluxioURI((mBasePath + p + "/" + redundent + "/" + i + ".txt").toString()), CreateFilePOptions.newBuilder().setRecursive(true).build()).close();
          }
          break;
        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }
  }

  private final class AlluxioFuseBenchThread extends BenchThread {
    java.nio.file.Path mFuseFixedBasePath;
    java.nio.file.Path mFuseBasePath;

    private AlluxioFuseBenchThread(BenchContext context) {
      super(context);
      mFuseFixedBasePath = Paths.get(String.valueOf(mFixedBasePath));
      mFuseBasePath = Paths.get(String.valueOf(mBasePath));
      try {
        Files.createDirectories(mFuseFixedBasePath);
        Files.createDirectories(mFuseBasePath);
      } catch (IOException e) {
        throw new RuntimeException(String.format("Failed to create directories %s %s",
            mFuseFixedBasePath, mFuseBasePath), e);
      }
    }

    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    protected void applyOperation(long counter) throws IOException {
      java.nio.file.Path path;
      switch (mParameters.mOperation) {
        case CREATE_DIR:
          if (counter < mParameters.mFixedCount) {
            path = mFuseFixedBasePath.resolve(Long.toString(counter));
          } else {
            path = mFuseBasePath.resolve(Long.toString(counter));
          }
          Files.createDirectories(path);
          break;
        case CREATE_FILE:
          if (counter < mParameters.mFixedCount) {
            path = mFuseFixedBasePath.resolve(Long.toString(counter));
          } else {
            path = mFuseBasePath.resolve(Long.toString(counter));
          }
          long fileSize = FormatUtils.parseSpaceSize(mParameters.mCreateFileSize);
          try (FileOutputStream stream = new FileOutputStream(String.valueOf(path))) {
            for (long i = 0; i < fileSize; i += StressConstants.WRITE_FILE_ONCE_MAX_BYTES) {
              stream.write(mFiledata, 0,
                  (int) Math.min(StressConstants.WRITE_FILE_ONCE_MAX_BYTES, fileSize - i));
            }
          }
          break;
        case GET_BLOCK_LOCATIONS:
          throw new UnsupportedOperationException("GET_BLOCK_LOCATIONS is not supported!");
        case GET_FILE_STATUS:
          counter = counter % mParameters.mFixedCount;
          path = mFuseFixedBasePath.resolve(Long.toString(counter));
          Files.readAttributes(path, BasicFileAttributes.class);
          break;
        case LIST_DIR:
          File dir = new File(mFuseFixedBasePath.toString());
          File[] files = dir.listFiles();
          if (files == null || files.length != mParameters.mFixedCount) {
            throw new IOException(String
                .format("listing `%s` expected %d files but got %d files", mFuseFixedBasePath,
                    mParameters.mFixedCount, files == null ? 0 : files.length));
          }
          break;
        case LIST_DIR_LOCATED:
          throw new UnsupportedOperationException("LIST_DIR_LOCATED is not supported!");
        case OPEN_FILE:
          counter = counter % mParameters.mFixedCount;
          path = mFuseFixedBasePath.resolve(Long.toString(counter));
          new FileInputStream(path.toString()).close();
          break;
        case RENAME_FILE:
          java.nio.file.Path dst;
          if (counter < mParameters.mFixedCount) {
            path = mFuseFixedBasePath.resolve(Long.toString(counter));
            dst = mFuseFixedBasePath.resolve(counter + "-renamed");
          } else {
            path = mFuseBasePath.resolve(Long.toString(counter));
            dst = mFuseBasePath.resolve(counter + "-renamed");
          }
          Files.move(path, dst);
          break;
        case DELETE_FILE:
          if (counter < mParameters.mFixedCount) {
            path = mFuseFixedBasePath.resolve(Long.toString(counter));
          } else {
            path = mFuseBasePath.resolve(Long.toString(counter));
          }
          Files.delete(path);
          break;
        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }
  }
}
