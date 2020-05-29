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

import alluxio.conf.InstancedConfiguration;
import alluxio.job.plan.PlanConfig;
import alluxio.stress.BaseParameters;
import alluxio.stress.job.IOConfig;
import alluxio.stress.worker.IOTaskResult;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.beust.jcommander.ParametersDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

  @ParametersDelegate
  private WorkerBenchParameters mParameters = new WorkerBenchParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();
  private final HashMap<String, String> mHdfsConf = new HashMap<>();

  @Override
  public PlanConfig generateJobConfig(String[] args) {
    // remove the cluster flag
    List<String> commandArgs =
            Arrays.stream(args).filter((s) -> !BaseParameters.CLUSTER_FLAG.equals(s))
                    .filter((s) -> !s.isEmpty()).collect(Collectors.toList());

    commandArgs.addAll(mBaseParameters.mJavaOpts);
    String className = this.getClass().getCanonicalName();
    return new IOConfig(className, commandArgs, mParameters);
  }

  @Override
  public IOTaskResult runLocal() throws Exception {
    ExecutorService pool =
            ExecutorServiceFactories.fixedThreadPool("bench-io-thread", mParameters.mThreads)
                    .create();

    IOTaskResult result = runIOBench(pool);

    pool.shutdownNow();
    pool.awaitTermination(30, TimeUnit.SECONDS);

    // Aggregate the task results
    return result;
  }

  @Override
  public void prepare() {
    // TODO(jiacheng): any HDFS conf to add?
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new UfsIOBench());
  }

  private String getFilePathStr(int idx) {
    return mParameters.mPath + String.format("io-benchmark-%d", idx);
  }

  private IOTaskResult runIOBench(ExecutorService pool) throws Exception {
    IOTaskResult writeTaskResult = write(pool);
    IOTaskResult readTaskResult = read(pool);
    cleanUp();
    return writeTaskResult.merge(readTaskResult);
  }

  private void cleanUp() throws IOException {
    UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(mConf)
            .createMountSpecificConf(mHdfsConf);
    UnderFileSystem ufs = UnderFileSystem.Factory.create(mParameters.mPath, ufsConf);

    for (int i = 0; i < mParameters.mThreads; i++) {
      ufs.deleteFile(getFilePathStr(i));
    }
  }

  private IOTaskResult read(ExecutorService pool)
          throws IOException, InterruptedException, ExecutionException {
    // Use multiple threads to saturate the bandwidth of this worker
    int numThreads = mParameters.mThreads;

    UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(mConf)
            .createMountSpecificConf(mHdfsConf);
    UnderFileSystem ufs = UnderFileSystem.Factory.create(mParameters.mPath, ufsConf);
    if (!ufs.exists(mParameters.mPath)) {
      // If the directory does not exist, there's no point proceeding
      throw new IOException(String.format("The target directory %s does not exist!",
              mParameters.mPath));
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

        int readMB = 0;
        InputStream inStream = null;
        try {
          inStream = ufs.open(filePath);
          byte[] buf = new byte[BUFFER_SIZE];
          while (readMB < mParameters.mDataSize && inStream.read(buf) > 0) {
            readMB += 1; // 1 MB
          }

          long endTime = CommonUtils.getCurrentMs();
          double duration = (endTime - startTime) / 1000.0; // convert to second
          IOTaskResult.Point p = new IOTaskResult.Point(IOConfig.IOMode.READ, duration, readMB);
          result.addPoint(p);
          LOG.debug("Read task finished {}", p);
        } catch (IOException e) {
          LOG.error("Failed to read {}", filePath, e);
          result.addError(e.getMessage());
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
          throws IOException, InterruptedException, ExecutionException {
    // Use multiple threads to saturate the bandwidth of this worker
    int numThreads = mParameters.mThreads;

    UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(mConf)
            .createMountSpecificConf(mHdfsConf);
    UnderFileSystem ufs = UnderFileSystem.Factory.create(mParameters.mPath, ufsConf);
    if (!ufs.exists(mParameters.mPath)) {
      LOG.debug("Prepare directory {}", mParameters.mPath);
      ufs.mkdirs(mParameters.mPath);
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
        LOG.debug("filePath={}, data to write={}MB", filePath, mParameters.mDataSize);

        int wroteMB = 0;
        BufferedOutputStream outStream = null;
        try {
          outStream = new BufferedOutputStream(ufs.create(filePath));
          while (wroteMB < mParameters.mDataSize) {
            outStream.write(randomData);
            wroteMB += 1; // 1 MB
          }
          outStream.flush();

          long endTime = CommonUtils.getCurrentMs();
          double duration = (endTime - startTime) / 1000.0; // convert to second
          IOTaskResult.Point p = new IOTaskResult.Point(IOConfig.IOMode.WRITE,
                  duration, wroteMB);
          result.addPoint(p);
          LOG.debug("Write task finished {}", p);
        } catch (IOException e) {
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
}
