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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.hadoop.HadoopConfigurationUtils;
import alluxio.stress.StressConstants;
import alluxio.stress.common.FileSystemClientType;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.stress.master.MasterBenchTaskResult;
import alluxio.stress.master.Operation;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

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
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Single node stress test.
 */
// TODO(jiacheng): avoid the implicit casts and @SuppressFBWarnings
public class StressMasterBench extends StressMasterBenchBase<MasterBenchTaskResult,
    MasterBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(StressMasterBench.class);

  /**
   * Creates instance.
   */
  public StressMasterBench() {
    super(new MasterBenchParameters());
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
        basePath = new Path(path, "dirs");
      } else {
        basePath = new Path(path, "files");
      }

      if (mParameters.mOperation == Operation.CREATE_FILE
          || mParameters.mOperation == Operation.CREATE_DIR) {
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
                  mParameters.mOperation));
        }
      }
      if (!prepareFs.getFileStatus(basePath).isDirectory()) {
        throw new IllegalStateException(String
            .format("base path (%s) must be a directory for operation (%s)", basePath,
                mParameters.mOperation));
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

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  protected BenchThread getBenchThread(BenchContext context, int index) {
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

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  protected StressMasterBenchBase<MasterBenchTaskResult, MasterBenchParameters>.BenchContext
      getContext() {
    RateLimiter rateLimiter = RateLimiter.create(mParameters.mTargetThroughput);
    return new BenchContext(
        rateLimiter, mParameters.mOperation, mParameters.mDuration);
  }

  @Override
  public void validateParams() throws Exception {
    // no-op
  }

  protected abstract class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final Histogram mResponseTimeNs;
    protected final Path mBasePath;
    protected final Path mFixedBasePath;

    private final MasterBenchTaskResult mResult = new MasterBenchTaskResult();

    private BenchThread(BenchContext context) {
      mContext = context;
      mResponseTimeNs = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
          StressConstants.TIME_HISTOGRAM_PRECISION);
      mBasePath = mContext.getBasePath(0);
      mFixedBasePath = mContext.getFixedBasePath(0);
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
        if (!useStopCount && CommonUtils.getCurrentMs() >= mContext.getEndMs()) {
          break;
        }
        localCounter = mContext.getOperationCounter(0).getAndIncrement();
        if (useStopCount && localCounter >= mParameters.mStopCount) {
          break;
        }

        mContext.getGrandRateLimiter().acquire();
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
      StressMasterBench.this.applyNativeOperation(
          mFs, mParameters.mOperation, counter,
          mBasePath, mFixedBasePath,
          mParameters.mFixedCount);
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
