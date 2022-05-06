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

package alluxio.stress.cli.client;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.stress.StressConstants;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.client.CompactionParameters;
import alluxio.stress.client.CompactionTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Benchmark that simulates a workload that compacts many small files into a bigger file.
 */
public class CompactionBench extends Benchmark<CompactionTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionBench.class);

  protected ExecutorService mPool = null;
  @ParametersDelegate
  protected final CompactionParameters mParameters = new CompactionParameters();
  protected FileSystem[] mCachedFs;
  private AlluxioURI mRealSourceBase;

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new CompactionBench());
  }

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A benchmark that simulates the workload of compacting many small files into a bigger "
            + "file.",
        "",
        "Example:",
        "# This example creates 4 source directories each containing 100 source files that are "
            + "10KB each,",
        "# and compacts every 10 source files into 1 output file, resulting in 40 output files in",
        "# a single output directory.",
        "# The compaction is done on 1 job worker with 5 threads, meaning each thread will "
            + "process 80 source",
        "# files in 8 sequential batches.",
        "$ bin/alluxio runClass alluxio.stress.cli.client.CompactionBench "
            + "--cluster "
            + "--cluster-limit 1 "
            + "--base alluxio:///compaction-base "
            + "--source-files 1000 "
            + "--source-dirs 4 "
            + "--source-file-size 10kb "
            + "--threads 5 "
            + "--compact-ratio 10 "
    ));
  }

  @Override
  public CompactionTaskResult runLocal() throws Exception {
    mCachedFs = new FileSystem[mParameters.mThreads];
    AlluxioProperties properties = getCustomProperties(mParameters.mCompactProperties);
    for (int i = 0; i < mParameters.mThreads; i++) {
      mCachedFs[i] = FileSystem.Factory.create(new InstancedConfiguration(properties));
    }
    FileSystem fs = mCachedFs[0];
    AlluxioURI baseUri = new AlluxioURI(mParameters.mBase);
    AlluxioURI destBaseUri = baseUri.join(mParameters.mOutputBase);
    AlluxioURI stagingBaseUri = baseUri.join(mParameters.mStagingBase);
    // Scan base dir to get all subdirectories that contain files to compact
    List<AlluxioURI> subDirs =
        fs.listStatus(mRealSourceBase)
            .stream()
            .filter(URIStatus::isFolder)
            .map(uri -> new AlluxioURI(mRealSourceBase, uri.getPath(), false))
            .collect(Collectors.toList());
    // Partition them into batches and each thread will work on one batch
    List<List<AlluxioURI>> partitions = exactPartition(subDirs, mParameters.mThreads);
    List<CompletableFuture<CompactionTaskResult>> futures = new ArrayList<>(mParameters.mThreads);
    try {
      for (int i = 0; i < mParameters.mThreads; i++) {
        List<AlluxioURI> partition = partitions.get(i);
        Map<AlluxioURI, AlluxioURI> srcDestDirMap = partition
            .stream()
            .collect(Collectors.toMap(
                src -> src,
                src -> mParameters.mOutputInPlace ? src : destBaseUri));
        BenchThread thread = new BenchThread(
            mCachedFs[i],
            srcDestDirMap,
            stagingBaseUri,
            mParameters.mCompactRatio,
            FormatUtils.parseTimeSize(mParameters.mDelayMs),
            (int) FormatUtils.parseSpaceSize(mParameters.mBufSize),
            mParameters.mPreserveSource,
            mParameters.mDeleteByDir,
            mBaseParameters.mId);
        CompletableFuture<CompactionTaskResult> future = CompletableFuture.supplyAsync(() -> {
          CompactionTaskResult result;
          try {
            result = thread.call();
          } catch (Exception e) {
            LOG.error("Failed to run compaction thread", e);
            result = new CompactionTaskResult();
            result.addError(e.getMessage());
          }
          return result;
        }, getPool());
        futures.add(future);
      }
      LOG.info("{} jobs submitted", futures.size());

      // Collect the results
      CompactionTaskResult result = new CompactionTaskResult();
      result.setBaseParameters(mBaseParameters);
      result.setParameters(mParameters);
      for (CompletableFuture<CompactionTaskResult> future : futures) {
        CompactionTaskResult threadResult = future.join();
        result.merge(threadResult);
      }
      return result;
    } catch (Exception e) {
      LOG.error("Failed to execute RPC in pool", e);
      CompactionTaskResult result = new CompactionTaskResult();
      result.setBaseParameters(mBaseParameters);
      result.setParameters(mParameters);
      result.addError(e.getMessage());
      return result;
    }
  }

  @Override
  public void prepare() throws Exception {
    Preconditions.checkArgument(mParameters.mThreads > 0, "mThreads");
    AlluxioProperties properties = getCustomProperties(mParameters.mPrepareProperties);
    FileSystem prepareFs = FileSystem.Factory.create(new InstancedConfiguration(properties));

    // flags:
    // distributed    cluster   in-process    run type
    //  1              1         any             n/a
    //  1              0         1               at job worker
    //  1              0         0               n/a
    //  0              1         1               n/a
    //  0              1         0               at the local process invoking the benchmark
    //                                             and will continue to run at job worker
    //  0              0         1               at the local forked process
    //  0              0         0               at the local process invoking the benchmark
    //                                             and will continue to run in forked process

    int flags = Boolean.compare(mBaseParameters.mDistributed, false) << 2
        | Boolean.compare(mBaseParameters.mCluster, false) << 1
        | Boolean.compare(mBaseParameters.mInProcess, false);
    switch (flags) {
      case 0b000:
      case 0b010:
        if (!mParameters.mSkipPrepare) {
          prepareSourceBaseDir(prepareFs);
          prepareOutputBaseDir(prepareFs);
          prepareStagingBaseDir(prepareFs);
        }
        break;
      case 0b001:
        // set real base to "local"
        mRealSourceBase =
            new AlluxioURI(mParameters.mBase).join(mParameters.mSourceBase).join("local");
        if (!mParameters.mSkipPrepare) {
          prepareSourceFiles(prepareFs);
        }
        break;
      case 0b101:
        // set real base to the id of the job worker, to avoid sharing the same base
        // path with other job workers
        mRealSourceBase = new AlluxioURI(mParameters.mBase)
            .join(mParameters.mSourceBase).join(mBaseParameters.mId);
        if (!mParameters.mSkipPrepare) {
          prepareSourceFiles(prepareFs);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Unknown combination of flags: %s",
            Integer.toBinaryString(flags)));
    }
  }

  private static AlluxioProperties getCustomProperties(List<String> propertyList) {
    AlluxioProperties properties = ConfigurationUtils.defaults();
    for (String property : propertyList) {
      String[] parts = property.split("=", 2);
      Preconditions.checkArgument(parts.length == 2,
          "Property should be set as \"key=value\", got %s", property);
      properties.set(PropertyKey.fromString(parts[0]), parts[1]);
    }
    return properties;
  }

  private void prepareStagingBaseDir(FileSystem fs) throws IOException, AlluxioException {
    try {
      fs.createDirectory(new AlluxioURI(mParameters.mBase).join(mParameters.mStagingBase),
          CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    } catch (FileAlreadyExistsException ignored) { /* ignored */ }
  }

  private void prepareOutputBaseDir(FileSystem fs) throws IOException, AlluxioException {
    if (!mParameters.mOutputInPlace) {
      AlluxioURI path = new AlluxioURI(mParameters.mBase).join(mParameters.mOutputBase);
      try {
        fs.delete(path,
            DeletePOptions.newBuilder().setRecursive(true).build());
      } catch (FileDoesNotExistException ignored) { /* ignored */ }
      fs.createDirectory(path,
          CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    }
  }

  private void prepareSourceBaseDir(FileSystem fs) throws IOException, AlluxioException {
    try {
      fs.createDirectory(new AlluxioURI(mParameters.mBase).join(mParameters.mSourceBase),
          CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    } catch (FileAlreadyExistsException ignored) { /* ignored */ }
  }

  private void prepareSourceFiles(FileSystem fs) throws Exception {
    final int fileSize = (int) FormatUtils.parseSpaceSize(mParameters.mSourceFileSize);
    // cap to 1 MB as the assumption is the source files are small
    final byte[] fileData = new byte[Math.min(fileSize, Constants.MB)];
    Arrays.fill(fileData, (byte) 0x7A);
    try {
      fs.createDirectory(mRealSourceBase);
    } catch (FileAlreadyExistsException ignored) { /* ignored */ }

    final AtomicInteger numDirsCreated = new AtomicInteger();
    int createFilesParallelism = Runtime.getRuntime().availableProcessors() * 2;
    ExecutorService pool = ExecutorServiceFactories
        .fixedThreadPool("compact-bench-prepare-thread", createFilesParallelism)
        .create();
    List<CompletableFuture<Exception>> futures = new ArrayList<>(createFilesParallelism);
    for (int i = 0; i < createFilesParallelism; i++) {
      CompletableFuture<Exception> future = CompletableFuture.supplyAsync(() -> {
        try {
          int localNumDirsCreated;
          while ((localNumDirsCreated = numDirsCreated.getAndIncrement())
              < mParameters.mNumSourceDirs) {
            LOG.info("creating directory {}/{} ",
                localNumDirsCreated, mParameters.mNumSourceDirs);
            AlluxioURI dir = mRealSourceBase.join(Integer.toString(localNumDirsCreated));
            try {
              fs.createDirectory(dir);
            } catch (FileAlreadyExistsException ignored) { /* ignored */ }

            for (int f = 0; f < mParameters.mNumSourceFiles; f++) {
              AlluxioURI path = dir.join(Integer.toString(f));
              try (FileOutStream stream = fs.createFile(path)) {
                for (long offset = 0; offset < fileSize; offset += fileData.length) {
                  stream.write(fileData, 0, (int) Math.min(fileData.length, fileSize - offset));
                }
              } catch (FileAlreadyExistsException e) {
                fs.delete(path);
                f--; // retry
              }
              // Print progress every 10% files have been created
              if (f % (mParameters.mNumSourceFiles / 10) == 0) {
                LOG.info("{}/{} files created in dir {}",
                    f, mParameters.mNumSourceFiles, localNumDirsCreated);
              }
            }
            LOG.info("{}/{} directories created",
                localNumDirsCreated, mParameters.mNumSourceDirs);
          }
        } catch (IOException | AlluxioException e) {
          return e;
        }
        return null;
      }, pool);
      futures.add(future);
    }
    try {
      for (CompletableFuture<Exception> future : futures) {
        Exception e = future.join();
        if (e != null) {
          LOG.error("Failed to prepare test directory and files", e);
          throw e;
        }
      }
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  @Override
  public void cleanup() throws Exception {
    super.cleanup();
    if (mPool != null) {
      LOG.debug("Terminating thread pool");
      mPool.shutdownNow();
      mPool.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * If the thread pool is not yet initialized, creates the pool.
   *
   * @return the thread pool
   */
  public ExecutorService getPool() {
    if (mPool == null) {
      mPool = ExecutorServiceFactories
          .fixedThreadPool("compact-benchmark-thread", mParameters.mThreads).create();
    }
    return mPool;
  }

  /**
   * Splits a list into exactly {@code numPartitions} partitions. Let {@code S = L / N}
   * and {@code R = L % N}, so that {@code L = S * N + R},
   * where {@code L} is the number of items in the list, and {@code N} is the number of partitions.
   * The first {@code R} partitions have {@code S+1} items, and the remaining {@code N - R}
   * partitions have {@code S} items.
   * @param list the list to partition
   * @param numPartitions number of partitions
   * @param <T> item type
   * @return a partitioned list of sub-lists views
   */
  private static <T> List<List<T>> exactPartition(List<T> list, int numPartitions) {
    int length = list.size();
    int sizePerPartition = length / numPartitions;
    int leftover = length % numPartitions;
    int leftoverEndIndex = (sizePerPartition + 1) * leftover;
    List<List<T>> partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < leftover; i++) {
      partitions.add(list.subList(i * (sizePerPartition + 1), (i + 1) * (sizePerPartition + 1)));
    }
    for (int i = 0; i < numPartitions - leftover; i++) {
      partitions.add(list.subList(leftoverEndIndex + i * sizePerPartition,
          leftoverEndIndex + (i + 1) * sizePerPartition));
    }
    return partitions;
  }

  static class BenchThread implements Callable<CompactionTaskResult> {
    private final FileSystem mFs;
    /* input dir to output dir mapping */
    private final Map<AlluxioURI, AlluxioURI> mSrcDestMap;
    private final AlluxioURI mStagingDir;
    private final int mCompactRatio;
    private final long mDelayMs;
    private final int mBufSize;
    private final boolean mPreserveSource;
    private final boolean mDeleteByDir;
    private final String mWorkerId;
    private final CompactionTaskResult mResult;
    private final Histogram mRawRecords;

    public BenchThread(FileSystem fs, Map<AlluxioURI, AlluxioURI> dirMap, AlluxioURI stagingDir,
                       int compactRatio, long delayMs, int bufSize,
                       boolean preserveSource, boolean deleteByDir, String workerId) {
      Preconditions.checkArgument(compactRatio >= 1, "compactRatio should be 1 or greater");
      Preconditions.checkArgument(delayMs >= 0, "delayMs should be 0 or greater");
      Preconditions.checkArgument(bufSize > 0, "buffer size should be greater than 0");
      mFs = fs;
      mSrcDestMap = dirMap;
      mStagingDir = stagingDir;
      mCompactRatio = compactRatio;
      mDelayMs = delayMs;
      mBufSize = bufSize;
      mPreserveSource = preserveSource;
      mDeleteByDir = deleteByDir;
      mWorkerId = workerId;
      mResult = new CompactionTaskResult();
      mRawRecords = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
          StressConstants.TIME_HISTOGRAM_PRECISION);
    }

    @Override
    public CompactionTaskResult call() throws Exception {
      runInternal();
      return mResult;
    }

    private void runInternal() throws Exception {
      Stopwatch stopwatch = Stopwatch.createUnstarted();
      for (Map.Entry<AlluxioURI, AlluxioURI> entry : mSrcDestMap.entrySet()) {
        // Gather all input files by listing this subdirectory
        AlluxioURI srcDir = entry.getKey();
        AlluxioURI destDir = entry.getValue();
        List<AlluxioURI> files =
            mFs.listStatus(srcDir, ListStatusPOptions.newBuilder().setRecursive(false).build())
                .stream()
                .filter(uri -> !uri.isFolder() && uri.isCompleted())
                .map(uri -> new AlluxioURI(srcDir, uri.getPath(), false))
                .collect(Collectors.toList());
        // Partition files into batches
        List<List<AlluxioURI>> batches = Lists.partition(files, mCompactRatio);
        LOG.info("Partitioned {} files in dir {} into {} batches, each with {} files",
            files.size(), srcDir, batches.size(), mCompactRatio);

        for (int i = 0; i < batches.size(); i++) {
          // Process files from one batch
          List<AlluxioURI> batch = batches.get(i);
          String outputFileName = String.format("compact_output_part%d_dir%s_worker%s",
              i, srcDir.getName(), mWorkerId);
          Compactor compactor =
              new Compactor(mFs, batch.iterator(), destDir, outputFileName, mStagingDir, mBufSize);

          try {
            stopwatch.reset();
            stopwatch.start();
            compactor.run();
            stopwatch.stop();
            mRawRecords.recordValue(stopwatch.elapsed(TimeUnit.NANOSECONDS));
            mResult.incrementNumSuccess();
          } catch (Exception e) {
            LOG.warn("Batch {} in dir {} failed", i + 1, e);
            mResult.addError(e.getMessage());
          }
          LOG.info("Batch {}/{} in dir {} finished", i + 1, batches.size(), srcDir);
          // Sleep as needed
          Thread.sleep(mDelayMs);
        }

        // Delete input files
        if (!mPreserveSource) {
          if (!mDeleteByDir) {
            for (AlluxioURI file : files) {
              mFs.delete(file);
            }
            mFs.delete(srcDir, DeletePOptions.newBuilder().build());
          } else {
            mFs.delete(srcDir, DeletePOptions.newBuilder().setRecursive(true).build());
          }
        }

        mResult.getStatistics().encodeResponseTimeNsRaw(mRawRecords);
      }
    }
  }

  static class Compactor {
    private final FileSystem mFs;
    private final AlluxioURI mOutputBase;
    private final String mOutputFileName;
    private final Iterator<AlluxioURI> mInputs;
    private final AlluxioURI mStagingBase;
    private final byte[] mBuffer;

    public Compactor(FileSystem fs, Iterator<AlluxioURI> inputs, AlluxioURI outputBase,
                     String outputFileName, AlluxioURI stagingBase, int bufSize) {
      mFs = fs;
      mOutputBase = outputBase;
      mOutputFileName = outputFileName;
      mInputs = inputs;
      mStagingBase = stagingBase;
      mBuffer = new byte[bufSize];
    }

    public void run() throws IOException, AlluxioException {
      // First write an intermediate file
      AlluxioURI tempFile = mStagingBase.join(CommonUtils.randomAlphaNumString(8));
      try (FileOutStream out = mFs.createFile(tempFile)) {
        while (mInputs.hasNext()) {
          try (FileInStream input = mFs.openFile(mInputs.next())) {
            int bytesRead;
            while ((bytesRead = input.read(mBuffer)) >= 0) {
              out.write(mBuffer, 0, bytesRead);
            }
          }
        }
      }
      // Move intermediate file to output base dir
      RetryPolicy retry = new CountingRetry(5);
      String nameSuffix = "";
      boolean done = false;
      while (retry.attempt()) {
        try {
          mFs.rename(tempFile, mOutputBase.join(mOutputFileName + nameSuffix));
          done = true;
          break;
        } catch (FileAlreadyExistsException ignored) {
          nameSuffix = "_" + retry.getAttemptCount();
        }
      }
      if (!done) {
        throw new FileAlreadyExistsException(
            String.format("Output file %s already exists, renaming failed after %d attempts",
                mOutputFileName, retry.getAttemptCount()));
      }
    }
  }
}
