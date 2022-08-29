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
//
//package alluxio.stress.cli;
//
//import alluxio.AlluxioURI;
//import alluxio.annotation.SuppressFBWarnings;
//import alluxio.client.file.FileSystem;
//import alluxio.exception.AlluxioException;
//import alluxio.grpc.CreateFilePOptions;
//import alluxio.grpc.WritePType;
//import alluxio.retry.CountingRetry;
//import alluxio.retry.RetryPolicy;
//import alluxio.stress.BaseParameters;
//import alluxio.stress.common.FileSystemClientType;
//import alluxio.stress.common.FileSystemParameters;
//import alluxio.stress.master.MasterBenchBaseParameters;
//import alluxio.stress.master.MasterBenchParameters;
//import alluxio.stress.master.MasterBenchTaskResult;
//import alluxio.stress.master.Operation;
//import alluxio.util.CommonUtils;
//import alluxio.util.FormatUtils;
//import alluxio.util.executor.ExecutorServiceFactories;
//import alluxio.util.io.PathUtils;
//
//import com.beust.jcommander.JCommander;
//import com.beust.jcommander.Strings;
//import org.apache.hadoop.fs.Path;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * MaxFile StressBench class.
// */
//@SuppressFBWarnings("BC_UNCONFIRMED_CAST")
//public class MaxFileBench extends StressMasterBench {
//  private static final Logger LOG = LoggerFactory.getLogger(MaxFileBench.class);
//
//  static AtomicBoolean sFinish = new AtomicBoolean(false);
//  private final MasterBenchTaskResult mTotalResults = new MasterBenchTaskResult();
//
//  private final List<String> mDefaultParams = Arrays.asList(
//      BaseParameters.BENCH_TIMEOUT, String.format("%ds", Integer.MAX_VALUE),
//      FileSystemParameters.CLIENT_TYPE_OPTION_NAME, FileSystemClientType.ALLUXIO_NATIVE.toString(),
//      BaseParameters.CLUSTER_START_DELAY_FLAG, "0s",
//      MasterBenchParameters.DURATION_OPTION_NAME, String.format("%ds", Integer.MAX_VALUE),
//      MasterBenchParameters.OPERATION_OPTION_NAME, Operation.CREATE_FILE.toString(),
//      MasterBenchBaseParameters.STOP_COUNT_OPTION_NAME, Integer.toString(
//          MasterBenchBaseParameters.STOP_COUNT_INVALID),
//      MasterBenchBaseParameters.WARMUP_OPTION_NAME, "0s",
//      FileSystemParameters.WRITE_TYPE_OPTION_NAME, WritePType.MUST_CACHE.toString()
//  );
//
//  /**
//   * @param args command-line arguments
//   */
//  public static void main(String[] args) {
//    mainInternal(args, new MaxFileBench());
//  }
//
//  @Override
//  public String getBenchDescription() {
//    List<String> descLines = new ArrayList<>(Arrays.asList(
//        "MaxFile. Creates files until no more files can be created.",
//        "This stressbench ignore the following options and sets its own values as follows:"
//    ));
//    for (int i = 0; i < mDefaultParams.size(); i += 2) {
//      descLines.add(String.format("%s=%s", mDefaultParams.get(i), mDefaultParams.get(i + 1)));
//    }
//    return Strings.join("\n\t", descLines);
//  }
//
//  @Override
//  protected void parseParameters(String[] args) {
//    List<String> argsList = new ArrayList<>(Arrays.asList(args));
//    argsList.addAll(mDefaultParams);
//
//    JCommander jc = new JCommander(this);
//    jc.setAllowParameterOverwriting(true);
//    jc.setProgramName(this.getClass().getSimpleName());
//    try {
//      jc.parse(argsList.toArray(new String[0]));
//      if (mBaseParameters.mHelp) {
//        System.out.println(getBenchDescription());
//        jc.usage();
//        System.exit(0);
//      }
//    } catch (Exception e) {
//      LOG.error("Failed to parse command: ", e);
//      System.out.println(getBenchDescription());
//      jc.usage();
//      throw e;
//    }
//  }
//
//  @Override
//  public MasterBenchTaskResult runLocal() throws Exception {
//    ExecutorService service = ExecutorServiceFactories.fixedThreadPool("maxfile-bench-thread",
//        mParameters.mThreads).create();
//
//    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
//    for (int i = 0; i < mParameters.mThreads; i++) {
//      callables.add(new AlluxioNativeMaxFileThread(i, mCachedNativeFs[i % mCachedNativeFs.length]));
//    }
//    LOG.info("Starting {} bench threads", callables.size());
//    long startMs = CommonUtils.getCurrentMs();
//    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
//        TimeUnit.MILLISECONDS);
//    mTotalResults.setDurationMs(CommonUtils.getCurrentMs() - startMs);
//    LOG.info("Bench threads finished");
//
//    service.shutdownNow();
//    service.awaitTermination(30, TimeUnit.SECONDS);
//
//    return mTotalResults;
//  }
//
//  private final class AlluxioNativeMaxFileThread implements Callable<Void> {
//    private static final int OPERATION_TIMEOUT_MS = 2 * 60 * 1_000;
//
//    private RetryPolicy mRetryPolicy = createPolicy();
//    private final ExecutorService mExecutor = Executors.newSingleThreadExecutor();
//    private final MasterBenchTaskResult mResult = new MasterBenchTaskResult();
//
//    private final int mId;
//    private final FileSystem mFs;
//    private final Path mBasePath;
//    private final Path mFixedBasePath;
//
//    AlluxioNativeMaxFileThread(int id, FileSystem fs) {
//      mId = id;
//      mFs = fs;
//      mBasePath = new Path(PathUtils.concatPath(mParameters.mBasePath, mFilesDir, mId));
//      mFixedBasePath = new Path(mBasePath, mFixedDir);
//      LOG.info("[{}]: basePath: {}, fixedBasePath: {}", mId, mBasePath, mFixedBasePath);
//    }
//
//    private RetryPolicy createPolicy() {
//      final int numAttempts = 3;
//      return new CountingRetry(numAttempts);
//    }
//
//    @Override
//    public Void call() throws Exception {
//      mResult.setRecordStartMs(CommonUtils.getCurrentMs());
//      AtomicLong localCounter = new AtomicLong();
//      while (!sFinish.get() && mRetryPolicy.attempt()) {
//        if (Thread.currentThread().isInterrupted()) {
//          break;
//        }
//        long increment = localCounter.getAndIncrement();
//        if (increment % 100_000 == 0) {
//          LOG.info("[{}] Created {} files", mId, increment);
//        }
//        try {
//          mExecutor.submit(() -> {
//            try {
//              applyOperation(increment);
//            } catch (Exception e) {
//              throw new RuntimeException(e);
//            }
//          }).get(OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
//          mResult.incrementNumSuccess(1);
//          mRetryPolicy = createPolicy();
//        } catch (Exception e) {
//          LOG.info("[{}] Attempt #{} failed: {}", mId, mRetryPolicy.getAttemptCount(), e);
//        }
//      }
//      sFinish.set(true);
//
//      // record local results
//      mResult.setEndMs(CommonUtils.getCurrentMs());
//      mResult.setParameters(mParameters);
//      mResult.setBaseParameters(mBaseParameters);
//
//      LOG.info("[{}] numSuccesses = {}", mId, mResult.getStatistics().mNumSuccess);
//
//      // merging total results
//      synchronized (mTotalResults) {
//        mTotalResults.merge(mResult);
//      }
//      return null;
//    }
//
//    private void applyOperation(long counter) throws IOException, AlluxioException {
//      Path path;
//      if (counter < mParameters.mFixedCount) {
//        path = new Path(mFixedBasePath, Long.toString(counter));
//      } else {
//        path = new Path(mBasePath, Long.toString(counter));
//      }
//
//      mFs.createFile(new AlluxioURI(path.toString()),
//          CreateFilePOptions.newBuilder().setRecursive(true).build());
//    }
//  }
//}
