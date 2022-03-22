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
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.stress.master.MasterBenchTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MaxFile StressBench class.
 */
public class MaxFileBench extends StressMasterBench {
  private static final Logger LOG = LoggerFactory.getLogger(MaxFileBench.class);

  static AtomicBoolean sFinish = new AtomicBoolean(false);
  private final MasterBenchTaskResult mTotalResults = new MasterBenchTaskResult();

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    // TODO(jenoudet): run MaxThroughput beforehand

    // TODO(jenoudet): integrate --operation --client-type into args automatically
    // bin/alluxio runClass alluxio.stress.cli.MaxFileBench --operation CreateDir \
    // --write-type MUST_CACHE --client-type AlluxioNative --threads 8 --in-process
    mainInternal(args, new MaxFileBench());
  }

  @Override
  public String getBenchDescription() {
    return "MaxFile stress bench";
  }

  // TODO(jenoudet): override JCommander description for MaxFile

  @Override
  public MasterBenchTaskResult runLocal() throws Exception {
    ExecutorService service = ExecutorServiceFactories.fixedThreadPool("maxfile-bench-thread",
        mParameters.mThreads).create();

    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
    for (int i = 0; i < mParameters.mThreads; i++) {
      callables.add(new AlluxioNativeMaxFileThread(i, mCachedNativeFs[i % mCachedNativeFs.length]));
    }
    LOG.info("Starting {} bench threads", callables.size());
    long startMs = CommonUtils.getCurrentMs();
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);
    mTotalResults.setDurationMs(CommonUtils.getCurrentMs() - startMs);
    LOG.info("Bench threads finished");

    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);

    return mTotalResults;
  }

  private final class AlluxioNativeMaxFileThread implements Callable<Void> {
    private final int mId;

    private final int mInitWaitTimeMs = 100;
    private final int mMaxWaitTimeMs = 15_000;
    private int mWaitTimeMs = mInitWaitTimeMs;
    private final ExecutorService mExecutor = Executors.newSingleThreadExecutor();

    private final MasterBenchTaskResult mResult = new MasterBenchTaskResult();
    private final FileSystem mFs;
    private final Path mBasePath;
    private final Path mFixedBasePath;

    AlluxioNativeMaxFileThread(int id, FileSystem fs) {
      mId = id;
      mFs = fs;
      mBasePath =
          new Path(PathUtils.concatPath(mParameters.mBasePath, "dirs", mId));
      mFixedBasePath = new Path(mBasePath, "fixed");
    }

    @Override
    public Void call() throws Exception {
      mResult.setRecordStartMs(CommonUtils.getCurrentMs());
      AtomicLong localCounter = new AtomicLong();
      while (!sFinish.get()) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        long increment = localCounter.getAndIncrement();
        if (increment % 100_000 == 0) {
          LOG.info("[{}] Created {} files\n", mId, increment);
        }
        try {
          mExecutor.submit(() -> {
            try {
              applyOperation(increment);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }).get(2_000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          if (mWaitTimeMs > mMaxWaitTimeMs) {
            LOG.info("[{}] Waiting timeout, ending: {}", mId, e);
            sFinish.set(true);
            break;
          } else {
            LOG.info("[{}] Failed, sleeping for {}: {}", mId, mWaitTimeMs, e);
            Thread.sleep(mWaitTimeMs);
            mWaitTimeMs *= 2;
            continue;
          }
        }
        mResult.incrementNumSuccess(1);
        mWaitTimeMs = mInitWaitTimeMs;
      }

      // record local results
      mResult.setEndMs(CommonUtils.getCurrentMs());
      mResult.setParameters(mParameters);
      mResult.setBaseParameters(mBaseParameters);

      LOG.info("[{}] numSuccesses = {}", mId, mResult.getStatistics().mNumSuccess);

      // merging total results
      synchronized (mTotalResults) {
        mTotalResults.merge(mResult);
      }
      return null;
    }

    private void applyOperation(long counter) throws IOException, AlluxioException {
      Path path;
      if (counter < mParameters.mFixedCount) {
        path = new Path(mFixedBasePath, Long.toString(counter));
      } else {
        path = new Path(mBasePath, Long.toString(counter));
      }

      mFs.createDirectory(new AlluxioURI(path.toString()),
          CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    }
  }
}
