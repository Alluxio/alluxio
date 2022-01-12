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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.stress.BaseParameters;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.cli.client.ClientIOWritePolicy;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.stress.worker.WorkerBenchTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.beust.jcommander.ParametersDelegate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Single node stress test.
 */
public class StressWorkerBench extends Benchmark<WorkerBenchTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(StressWorkerBench.class);

  @ParametersDelegate
  private WorkerBenchParameters mParameters = new WorkerBenchParameters();

  private FileSystem[] mCachedFs;
  private Path mFilePath;

  /**
   * Creates instance.
   */
  public StressWorkerBench() {
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new StressWorkerBench());
  }

  @Override
  public String getBenchDescription() {
    // TODO(David) Fill in description
    return "";
  }

  @Override
  public void prepare() throws Exception {
    mFilePath = new Path(mParameters.mBasePath, "data");

    // Read and write to one worker
    ClientIOWritePolicy.setMaxWorkers(1);

    if (!mBaseParameters.mDistributed) {
      // set hdfs conf for preparation client
      Configuration hdfsConf = new Configuration();
      // force delete, create dirs through to UFS
      hdfsConf.set(PropertyKey.Name.USER_FILE_DELETE_UNCHECKED, "true");
      hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");
      hdfsConf.set(PropertyKey.Name.USER_BLOCK_WRITE_LOCATION_POLICY,
          ClientIOWritePolicy.class.getName());
      hdfsConf.set(PropertyKey.Name.USER_UFS_BLOCK_READ_LOCATION_POLICY,
          ClientIOWritePolicy.class.getName());
      FileSystem prepareFs = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);

      // initialize the base, for only the non-distributed task (the cluster launching task)
      Path path = new Path(mParameters.mBasePath);

      prepareFs.delete(path, true);
      prepareFs.mkdirs(path);

      int fileSize = (int) FormatUtils.parseSpaceSize(mParameters.mFileSize);
      byte[] buffer = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mBufferSize)];
      Arrays.fill(buffer, (byte) 'A');

      try (FSDataOutputStream mOutStream = prepareFs
          .create(mFilePath, false, buffer.length, (short) 1,
              FormatUtils.parseSpaceSize(mParameters.mBlockSize))) {
        while (true) {
          int bytesToWrite = (int) Math.min(fileSize - mOutStream.getPos(), buffer.length);
          if (bytesToWrite == 0) {
            break;
          }
          mOutStream.write(buffer, 0, bytesToWrite);
        }
      }

      if (mParameters.mFree && Constants.SCHEME.equals(mFilePath.toUri().getScheme())) {
        // free the alluxio file
        alluxio.client.file.FileSystem.Factory.get().free(new AlluxioURI(mFilePath.toString()));
        LOG.info("Freed file before reading: " + mFilePath);
      }
    }

    // set hdfs conf for all test clients
    Configuration hdfsConf = new Configuration();
    // do not cache these clients
    hdfsConf.set(
        String.format("fs.%s.impl.disable.cache", (new URI(mParameters.mBasePath)).getScheme()),
        "true");
    hdfsConf.set(PropertyKey.Name.USER_BLOCK_WRITE_LOCATION_POLICY,
        ClientIOWritePolicy.class.getName());
    hdfsConf.set(PropertyKey.Name.USER_UFS_BLOCK_READ_LOCATION_POLICY,
        ClientIOWritePolicy.class.getName());
    for (Map.Entry<String, String> entry : mParameters.mConf.entrySet()) {
      hdfsConf.set(entry.getKey(), entry.getValue());
    }
    mCachedFs = new FileSystem[mParameters.mClients];
    for (int i = 0; i < mCachedFs.length; i++) {
      mCachedFs[i] = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);
    }
  }

  @Override
  public WorkerBenchTaskResult runLocal() throws Exception {
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mThreads).create();

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (mBaseParameters.mStartMs == BaseParameters.UNDEFINED_START_MS) {
      startMs = CommonUtils.getCurrentMs() + 5000;
    }
    long endMs = startMs + warmupMs + durationMs;
    BenchContext context = new BenchContext(startMs, endMs);

    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
    for (int i = 0; i < mParameters.mThreads; i++) {
      callables.add(new BenchThread(context, mCachedFs[i % mCachedFs.length]));
    }
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);

    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);

    return context.getResult();
  }

  private final class BenchContext {
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
    private final FileSystem mFs;
    private final byte[] mBuffer;
    private final WorkerBenchTaskResult mResult;

    private FSDataInputStream mInStream = null;

    private BenchThread(BenchContext context, FileSystem fs) {
      mContext = context;
      mFs = fs;
      mBuffer = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mBufferSize)];

      mResult = new WorkerBenchTaskResult();
      mResult.setParameters(mParameters);
      mResult.setBaseParameters(mBaseParameters);
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
        int ioBytes = applyOperation();

        long currentMs = CommonUtils.getCurrentMs();
        // Start recording after the warmup
        if (currentMs > recordMs) {
          if (ioBytes > 0) {
            mResult.incrementIOBytes(ioBytes);
          }
        }
      }
    }

    private int applyOperation() throws IOException {
      if (mInStream == null) {
        mInStream = mFs.open(mFilePath);
      }

      int bytesRead = mInStream.read(mBuffer);
      if (bytesRead < 0) {
        closeInStream();
        mInStream = mFs.open(mFilePath);
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
