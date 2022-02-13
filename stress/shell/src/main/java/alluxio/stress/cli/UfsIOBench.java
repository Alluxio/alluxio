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

import alluxio.cli.ValidationUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.stress.worker.IOTaskResult;
import alluxio.stress.worker.UfsIOParameters;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A benchmark tool measuring the IO to UFS.
 * */
public class UfsIOBench extends Benchmark<IOTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(UfsIOBench.class);
  private static final int BUFFER_SIZE = 1024 * 1024;
  private static final String TEST_DIR_NAME = "UfsIOTest";

  @ParametersDelegate
  private UfsIOParameters mParameters = new UfsIOParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private final UUID mTaskId = UUID.randomUUID();

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A benchmarking tool for the I/O between Alluxio and UFS.",
        "This test will measure the I/O throughput between Alluxio workers and "
            + "the specified UFS path. Each worker will create concurrent clients to "
            + "first generate test files of the specified size then read those files. "
            + "The write/read I/O throughput will be measured in the process.",
        "",
        "Example:",
        "# This invokes the I/O benchmark to HDFS in the Alluxio cluster",
        "# 2 workers will be used",
        "# 2 concurrent clients will be created on each worker",
        "# Each thread is writing then reading 512m of data",
        "$ bin/alluxio runUfsIOTest --path hdfs://<hdfs-address> --cluster --cluster-limit 2 \\",
        " --io-size 512m --threads 2",
        ""
    ));
  }

  @Override
  public IOTaskResult runLocal() throws Exception {
    // UfsIOBench is invoked from the job worker then runs in a standalone process
    // The process type should be set to keep consistent
    boolean switched = CommonUtils.PROCESS_TYPE.compareAndSet(CommonUtils.ProcessType.CLIENT,
        CommonUtils.ProcessType.JOB_WORKER);

    LOG.debug("Running locally with {} threads", mParameters.mThreads);
    ExecutorService pool = null;
    IOTaskResult result = null;
    try {
      pool = ExecutorServiceFactories.fixedThreadPool("bench-io-thread", mParameters.mThreads)
                      .create();

      result = runIOBench(pool);
      LOG.debug("IO benchmark finished with result: {}", result);
      // Aggregate the task results
      return result;
    } catch (Exception e) {
      if (result == null) {
        LOG.error("Failed run UFS IO benchmark on path {}", mParameters.mPath, e);
        result = new IOTaskResult();
        result.setParameters(mParameters);
        result.setBaseParameters(mBaseParameters);
        result.addError(ValidationUtils.getErrorInfo(e));
      }
      return result;
    } finally {
      if (pool != null) {
        pool.shutdownNow();
        pool.awaitTermination(30, TimeUnit.SECONDS);
      }
      if (switched) {
        // restore the process type if it was switched
        CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.CLIENT);
      }
    }
  }

  @Override
  public void prepare() {
    if (mParameters.mUseUfsConf && !mBaseParameters.mCluster) {
      throw new IllegalArgumentException(String.format(
          "%s can not use the ufs conf if it is not running in cluster mode",
          getClass().getName()));
    }
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new UfsIOBench());
  }

  private String getFilePathStr(int idx) {
    return mParameters.mPath + String.format("io-benchmark-%s-%d", mTaskId.toString(), idx);
  }

  private IOTaskResult runIOBench(ExecutorService pool) throws Exception {
    IOTaskResult writeTaskResult = write(pool);
    if (writeTaskResult.getPoints().size() == 0) {
      LOG.error("Failed to write any files. Abort the test.");
      return writeTaskResult;
    }
    IOTaskResult readTaskResult = read(pool);
    cleanUp();
    return writeTaskResult.merge(readTaskResult);
  }

  private void cleanUp() throws IOException {
    UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(mConf)
            .createMountSpecificConf(mParameters.mConf);
    UnderFileSystem ufs = UnderFileSystem.Factory.create(mParameters.mPath, ufsConf);

    for (int i = 0; i < mParameters.mThreads; i++) {
      ufs.deleteFile(getFilePathStr(i));
    }
  }

  private IOTaskResult read(ExecutorService pool)
          throws InterruptedException, ExecutionException {
    UnderFileSystemConfiguration ufsConf;
    UnderFileSystem ufs;
    int numThreads;
    long ioSizeBytes;
    String dataDirPath = getDataDirPath(mParameters.mPath);
    try {
      // Use multiple threads to saturate the bandwidth of this worker
      numThreads = mParameters.mThreads;
      ioSizeBytes = FormatUtils.parseSpaceSize(mParameters.mDataSize);
      ufsConf = UnderFileSystemConfiguration.defaults(mConf)
              .createMountSpecificConf(mParameters.mConf);
      ufs = UnderFileSystem.Factory.create(dataDirPath, ufsConf);
      if (!ufs.exists(dataDirPath)) {
        // If the directory does not exist, there's no point proceeding
        throw new IOException(String.format("The target directory %s does not exist!",
                dataDirPath));
      }
    } catch (Exception e) {
      LOG.error("Failed to access UFS path {}", dataDirPath);
      // If the UFS path is not valid, abort the test
      IOTaskResult result = new IOTaskResult();
      result.setParameters(mParameters);
      result.setBaseParameters(mBaseParameters);
      result.addError(ValidationUtils.getErrorInfo(e));
      return result;
    }

    List<CompletableFuture<IOTaskResult>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      CompletableFuture<IOTaskResult> future = CompletableFuture.supplyAsync(() -> {
        IOTaskResult result = new IOTaskResult();
        result.setBaseParameters(mBaseParameters);
        result.setParameters(mParameters);
        long startTime = CommonUtils.getCurrentMs();

        String filePath = getFilePathStr(idx);
        LOG.debug("Reading filePath={}", filePath);

        long readBytes = 0;
        InputStream inStream = null;
        try {
          inStream = ufs.open(filePath);
          byte[] buf = new byte[BUFFER_SIZE];
          int readBufBytes;
          while (readBytes < ioSizeBytes && (readBufBytes = inStream.read(buf)) > 0) {
            readBytes += readBufBytes;
          }

          long endTime = CommonUtils.getCurrentMs();
          double duration = (endTime - startTime) / 1000.0; // convert to second
          IOTaskResult.Point p = new IOTaskResult.Point(IOTaskResult.IOMode.READ,
                  duration, readBytes);
          result.addPoint(p);
          LOG.debug("Read task finished {}", p);
        } catch (Exception e) {
          LOG.error("Failed to read {}", filePath, e);
          result.addError(ValidationUtils.getErrorInfo(e));
        } finally {
          if (inStream != null) {
            try {
              inStream.close();
            } catch (IOException e) {
              LOG.warn("Failed to close read stream {}", filePath, e);
              result.addError(e.getMessage());
            }
          }
        }

        return result;
      }, pool);
      futures.add(future);
    }

    // Collect the result
    CompletableFuture[] cfs = futures.toArray(new CompletableFuture[0]);
    List<IOTaskResult> results = CompletableFuture.allOf(cfs)
            .thenApply(f -> futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList())
            ).get();

    return IOTaskResult.reduceList(results);
  }

  private IOTaskResult write(ExecutorService pool)
          throws InterruptedException, ExecutionException {
    UnderFileSystemConfiguration ufsConf;
    UnderFileSystem ufs;
    int numThreads;
    long ioSizeBytes;
    try {
      // Use multiple threads to saturate the bandwidth of this worker
      numThreads = mParameters.mThreads;
      ioSizeBytes = FormatUtils.parseSpaceSize(mParameters.mDataSize);
      ufsConf = UnderFileSystemConfiguration.defaults(mConf)
              .createMountSpecificConf(mParameters.mConf);
      // Create a subdir for the IO
      String dataDirPath = getDataDirPath(mParameters.mPath);
      ufs = UnderFileSystem.Factory.create(dataDirPath, ufsConf);
      if (!ufs.exists(dataDirPath)) {
        LOG.debug("Prepare directory {}", dataDirPath);
        ufs.mkdirs(dataDirPath);
      }
    } catch (Exception e) {
      LOG.error("Failed to prepare directory {} under UFS path {}", TEST_DIR_NAME,
              mParameters.mPath);
      // If the UFS path is not valid, abort the test
      IOTaskResult result = new IOTaskResult();
      result.setParameters(mParameters);
      result.setBaseParameters(mBaseParameters);
      result.addError(ValidationUtils.getErrorInfo(e));
      return result;
    }

    List<CompletableFuture<IOTaskResult>> futures = new ArrayList<>();
    final byte[] randomData = CommonUtils.randomBytes(BUFFER_SIZE);
    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      CompletableFuture<IOTaskResult> future = CompletableFuture.supplyAsync(() -> {
        IOTaskResult result = new IOTaskResult();
        result.setParameters(mParameters);
        result.setBaseParameters(mBaseParameters);
        long startTime = CommonUtils.getCurrentMs();

        String filePath = getFilePathStr(idx);
        LOG.debug("filePath={}, data to write={}", filePath, mParameters.mDataSize);

        long wroteBytes = 0;
        BufferedOutputStream outStream = null;
        try {
          outStream = new BufferedOutputStream(ufs.create(filePath));
          while (wroteBytes < ioSizeBytes) {
            long bytesToWrite = Math.min(ioSizeBytes - wroteBytes, BUFFER_SIZE);
            // bytesToWrite is bounded by BUFFER_SIZE, which is an integer
            outStream.write(randomData, 0, (int) bytesToWrite);
            wroteBytes += bytesToWrite;
          }
          outStream.flush();

          long endTime = CommonUtils.getCurrentMs();
          double duration = (endTime - startTime) / 1000.0; // convert to second
          IOTaskResult.Point p = new IOTaskResult.Point(IOTaskResult.IOMode.WRITE,
                  duration, wroteBytes);
          result.addPoint(p);
          LOG.debug("Write task finished {}", p);
        } catch (Exception e) {
          LOG.error("Failed to write to UFS: ", e);
          result.addError(e.getMessage());
        } finally {
          if (outStream != null) {
            try {
              outStream.close();
            } catch (IOException e) {
              LOG.warn("Failed to close stream to UFS: ", e);
              result.addError(e.getMessage());
            }
          }
        }

        LOG.debug("Thread {} file={}, IOBench result={}", Thread.currentThread().getName(),
                filePath, result);
        return result;
      }, pool);
      futures.add(future);
    }

    // Collect the result
    CompletableFuture[] cfs = futures.toArray(new CompletableFuture[0]);
    List<IOTaskResult> results = CompletableFuture.allOf(cfs)
            .thenApply(f -> futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList())
            ).get();

    return IOTaskResult.reduceList(results);
  }

  private static String getDataDirPath(String path) {
    return PathUtils.concatPath(path, TEST_DIR_NAME);
  }
}
