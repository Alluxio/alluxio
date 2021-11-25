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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.stress.StressConstants;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.client.CompactionParameters;
import alluxio.stress.client.CompactionTaskResult;
import alluxio.util.ConfigurationUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Compaction extends Benchmark<CompactionTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(Compaction.class);
  // TODO(bowen): set according to Alluxio configuration
  private static final int BUF_SIZE = 4096;

  protected ExecutorService mPool = null;
  @ParametersDelegate
  protected final CompactionParameters mParameters = new CompactionParameters();
  protected FileSystem[] mCachedFs;

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new Compaction());
  }

  @Override
  public String getBenchDescription() {
    return null;
  }

  @Override
  public CompactionTaskResult runLocal() throws Exception {
    mCachedFs = new FileSystem[mParameters.mThreads];
    AlluxioProperties properties = ConfigurationUtils.defaults();
    for (int i = 0; i < mParameters.mThreads; i++) {
      mCachedFs[i] = FileSystem.Factory.create(new InstancedConfiguration(properties));
    }
    FileSystem fs = mCachedFs[0];
    AlluxioURI srcBaseUri = new AlluxioURI(mParameters.mSourceBase);
    AlluxioURI destBaseUri = new AlluxioURI(mParameters.mOutputBase);
    // Scan base dir to get all subdirectories that contain files to compact
    List<AlluxioURI> subDirs =
        fs.listStatus(srcBaseUri)
            .stream()
            .filter(URIStatus::isFolder)
            .map(uri -> new AlluxioURI(srcBaseUri, uri.getPath(), false))
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
        BenchThread thread = new BenchThread(mCachedFs[i], srcDestDirMap, mParameters.mCompactRatio,
            mParameters.mDelayMs, mParameters.mPreserveSource);
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

    if (!mBaseParameters.mDistributed) {
      AlluxioProperties properties = ConfigurationUtils.defaults();
      properties.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");
      FileSystem prepareFs = FileSystem.Factory.create(new InstancedConfiguration(properties));
      // Make sure the destination dir exists
      if (!mParameters.mOutputInPlace) {
        try {
          prepareFs.createDirectory(new AlluxioURI(mParameters.mOutputBase));
        } catch (FileAlreadyExistsException ignored) {}
      }
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
    private final int mCompactRatio;
    private final int mDelayMs;
    private final boolean mPreserveSource;
    private CompactionTaskResult mResult;
    private Histogram mRawRecords;

    public BenchThread(FileSystem fs, Map<AlluxioURI, AlluxioURI> dirMap,
                       int compactRatio, int delayMs, boolean preserveSource) {
      Preconditions.checkArgument(compactRatio >= 1, "compactRatio should be 1 or greater");
      Preconditions.checkArgument(delayMs >= 0, "delayMs should be 0 or greater");
      mFs = fs;
      mSrcDestMap = dirMap;
      mCompactRatio = compactRatio;
      mDelayMs = delayMs;
      mPreserveSource = preserveSource;
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
          List<FileInStream> inputs = new ArrayList<>(batch.size());
          for (AlluxioURI file : batch) {
            FileInStream inStream = mFs.openFile(file);
            inputs.add(inStream);
          }
          RetryPolicy retry = new CountingRetry(5);
          FileOutStream output = null;
          String outputFileName = String.format("compact_output_%d_%s", i, srcDir.getName());
          while (retry.attempt()) {
            try {
              output = mFs.createFile(
                  new AlluxioURI(PathUtils.concatPath(destDir, outputFileName)));
              break;
            } catch (FileAlreadyExistsException ignored) {
              outputFileName = String.format("compact_output_%d_%s_%d",
                  i, srcDir.getName(), retry.getAttemptCount());
            }
          }
          if (output == null) {
            throw new FileAlreadyExistsException(
                String.format("Output file %s already exists, "
                    + "renaming failed after 5 attempts", outputFileName));
          }

          Compactor compactor = new Compactor(inputs.iterator(), output, BUF_SIZE);

          LOG.info("Starting batch {}/{}", i, batches.size());
          try {
            stopwatch.reset();
            stopwatch.start();
            compactor.run();
            stopwatch.stop();
            mRawRecords.recordValue(stopwatch.elapsed(TimeUnit.NANOSECONDS));
            mResult.incrementNumSuccess();
          } catch (IOException e) {
            LOG.warn("Batch {} in dir {} failed", i, e);
            mResult.addError(e.getMessage());
          }
          // Sleep as needed
          Thread.sleep(mDelayMs);
        }

        // Delete input files
        if (!mPreserveSource) {
          for (AlluxioURI file : files) {
            mFs.delete(file);
          }
        }

        mResult.getStatistics().encodeResponseTimeNsRaw(mRawRecords);
      }
    }
  }

  static class Compactor {
    private final FileOutStream mOutput;
    private final Iterator<FileInStream> mInputs;
    private final byte[] mBuffer;

    public Compactor(Iterator<FileInStream> inputs, FileOutStream output, int bufSize) {
      mOutput = output;
      mInputs = inputs;
      mBuffer = new byte[bufSize];
    }

    public void run() throws IOException {
      while (mInputs.hasNext()) {
        FileInStream input = mInputs.next();
        int bytesRead;
        while ((bytesRead = input.read(mBuffer)) > 0) {
          mOutput.write(mBuffer, 0, bytesRead);
        }
        input.close();
      }
      mOutput.close();
    }
  }
}
