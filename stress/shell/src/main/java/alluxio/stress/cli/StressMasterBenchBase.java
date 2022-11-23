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
import alluxio.exception.AlluxioException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.common.FileSystemClientType;
import alluxio.stress.master.MasterBenchBaseParameters;
import alluxio.stress.master.MasterBenchTaskResultBase;
import alluxio.stress.master.MasterBenchTaskResultStatistics;
import alluxio.stress.master.Operation;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The base class for master stress bench tests.
 *
 * @param <T> the type of task result
 * @param <P> the type of task parameter
 */
// TODO(jiacheng): avoid the implicit casts and @SuppressFBWarnings
public abstract class StressMasterBenchBase
    <T extends MasterBenchTaskResultBase<P>, P extends MasterBenchBaseParameters>
    extends AbstractStressBench<T, P> {
  private static final Logger LOG = LoggerFactory.getLogger(StressMasterBenchBase.class);

  protected byte[] mFiledata;

  /** Cached FS instances. */
  protected FileSystem[] mCachedFs;

  /** In case the Alluxio Native API is used,  use the following instead. */
  protected alluxio.client.file.FileSystem[] mCachedNativeFs;
  /* Directories where the stress bench creates files depending on the --operation chosen. */
  protected final String mDirsDir = "dirs";
  protected final String mFilesDir = "files";
  protected final String mFixedDir = "fixed";

  /**
   * Creates instance.
   */
  protected StressMasterBenchBase(P parameters) {
    mParameters = parameters;
  }

  protected abstract BenchContext getContext();

  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  protected void deletePaths(FileSystem fs, Path basePath) throws Exception {
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
    Path fixedPath = new Path(subDirs[0].getPath(), "fixed");
    int fixedSize = fs.exists(fixedPath) ? fs.listStatus(fixedPath).length : 0;

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
  public T runLocal() throws Exception {
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mThreads).create();

    long fileSize = FormatUtils.parseSpaceSize(mParameters.mCreateFileSize);
    mFiledata = new byte[(int) Math.min(fileSize, StressConstants.WRITE_FILE_ONCE_MAX_BYTES)];
    Arrays.fill(mFiledata, (byte) 0x7A);

    BenchContext context = getContext();

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

  protected abstract Callable<Void> getBenchThread(BenchContext context, int index);

  protected final class BenchContext {
    private final RateLimiter mGrandRateLimiter;
    private final RateLimiter[] mOperationRateLimiters;
    private final long mStartMs;
    private final long mEndMs;
    private final AtomicLong[] mOperationCounters;
    private final AtomicLong mTotalCounter;
    private final Path[] mBasePaths;
    private final Path[] mFixedBasePaths;

    /** The results. Access must be synchronized for thread safety. */
    private T mResult;

    BenchContext(
        RateLimiter grandRateLimiter, RateLimiter[] rateLimiters,
        Operation[] operations, String[] basePaths, String duration) {
      mGrandRateLimiter = grandRateLimiter;
      mOperationRateLimiters = rateLimiters;
      long durationMs = FormatUtils.parseTimeSize(duration);
      long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
      long startMs = mBaseParameters.mStartMs;
      if (mBaseParameters.mStartMs == BaseParameters.UNDEFINED_START_MS) {
        startMs = CommonUtils.getCurrentMs() + 1000;
      }
      mStartMs = startMs;
      mEndMs = startMs + warmupMs + durationMs;

      mOperationCounters = new AtomicLong[operations.length];
      mTotalCounter = new AtomicLong();

      mBasePaths = new Path[operations.length];
      mFixedBasePaths = new Path[operations.length];

      for (int i = 0; i < operations.length; i++) {
        mOperationCounters[i] = new AtomicLong();
        if (operations[i] == Operation.CREATE_DIR) {
          mBasePaths[i] =
              new Path(PathUtils.concatPath(basePaths[i], mDirsDir, mBaseParameters.mId));
        } else {
          mBasePaths[i] =
              new Path(PathUtils.concatPath(basePaths[i], mFilesDir, mBaseParameters.mId));
        }
        mFixedBasePaths[i] = new Path(mBasePaths[i], mFixedDir);
        LOG.info(
            "BenchContext: basePath: {}, fixedBasePath: {}", mBasePaths[i], mFixedBasePaths[i]);
      }
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    public BenchContext(RateLimiter rateLimiter, Operation operation, String duration) {
      this(rateLimiter, new RateLimiter[]{rateLimiter},
          new Operation[]{operation}, new String[]{mParameters.mBasePath}, duration);
    }

    public RateLimiter[] getRateLimiters() {
      return mOperationRateLimiters;
    }

    public RateLimiter getGrandRateLimiter() {
      return mGrandRateLimiter;
    }

    public RateLimiter getRateLimiter(int index) {
      return mOperationRateLimiters[index];
    }

    public long getStartMs() {
      return mStartMs;
    }

    public long getEndMs() {
      return mEndMs;
    }

    public AtomicLong getOperationCounter(int index) {
      return mOperationCounters[index];
    }

    AtomicLong getTotalCounter() {
      return mTotalCounter;
    }

    public Path getBasePath(int index) {
      return mBasePaths[index];
    }

    Path[] getBasePaths() {
      return mBasePaths;
    }

    Path[] getFixedPaths() {
      return mFixedBasePaths;
    }

    public Path getFixedBasePath(int index) {
      return mFixedBasePaths[index];
    }

    public synchronized void mergeThreadResult(T threadResult) {
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

    public synchronized T getResult() {
      return mResult;
    }
  }

  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  protected void applyNativeOperation(
      alluxio.client.file.FileSystem fs, Operation operation, long counter,
      Path basePath, Path fixedBasePath, int fixedCount)
      throws IOException, AlluxioException {
    Path path;
    switch (operation) {
      case CREATE_DIR:
        if (counter < fixedCount) {
          path = new Path(fixedBasePath, Long.toString(counter));
        } else {
          path = new Path(basePath, Long.toString(counter));
        }

        fs.createDirectory(new AlluxioURI(path.toString()),
            CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
        break;
      case CREATE_FILE:
        if (counter < fixedCount) {
          path = new Path(fixedBasePath, Long.toString(counter));
        } else {
          path = new Path(basePath, Long.toString(counter));
        }
        long fileSize = FormatUtils.parseSpaceSize(mParameters.mCreateFileSize);
        try (FileOutStream stream = fs.createFile(new AlluxioURI(path.toString()),
            CreateFilePOptions.newBuilder().setRecursive(true).build())) {
          for (long i = 0; i < fileSize; i += StressConstants.WRITE_FILE_ONCE_MAX_BYTES) {
            stream.write(mFiledata, 0,
                (int) Math.min(StressConstants.WRITE_FILE_ONCE_MAX_BYTES, fileSize - i));
          }
        }
        break;
      case GET_BLOCK_LOCATIONS:
        counter = counter % fixedCount;
        path = new Path(fixedBasePath, Long.toString(counter));
        fs.getBlockLocations(new AlluxioURI(path.toString()));
        break;
      case GET_FILE_STATUS:
        counter = counter % fixedCount;
        path = new Path(fixedBasePath, Long.toString(counter));
        fs.getStatus(new AlluxioURI(path.toString()));
        break;
      case LIST_DIR:
        List<alluxio.client.file.URIStatus> files
            = fs.listStatus(new AlluxioURI(fixedBasePath.toString()));
        if (files.size() != fixedCount) {
          throw new IOException(String
              .format("listing `%s` expected %d files but got %d files", fixedBasePath,
                  fixedCount, files.size()));
        }
        break;
      case LIST_DIR_LOCATED:
        throw new UnsupportedOperationException("LIST_DIR_LOCATED is not supported!");
      case OPEN_FILE:
        counter = counter % fixedCount;
        path = new Path(fixedBasePath, Long.toString(counter));
        fs.openFile(new AlluxioURI(path.toString())).close();
        break;
      case RENAME_FILE:
        if (counter < fixedCount) {
          path = new Path(fixedBasePath, Long.toString(counter));
        } else {
          path = new Path(basePath, Long.toString(counter));
        }
        Path dst = new Path(path + "-renamed");
        fs.rename(new AlluxioURI(path.toString()), new AlluxioURI(dst.toString()));
        break;
      case DELETE_FILE:
        if (counter < fixedCount) {
          path = new Path(fixedBasePath, Long.toString(counter));
        } else {
          path = new Path(basePath, Long.toString(counter));
        }

        fs.delete(new AlluxioURI(path.toString()),
            DeletePOptions.newBuilder().setRecursive(false).build());
        break;
      default:
        throw new IllegalStateException("Unknown operation: " + operation);
    }
  }
}
