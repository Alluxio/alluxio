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

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.client.ClientIOOperation;
import alluxio.stress.client.ClientIOParameters;
import alluxio.stress.client.ClientIOTaskResult;
import alluxio.stress.common.SummaryStatistics;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.beust.jcommander.ParametersDelegate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Single node client IO stress test.
 */
public class StressClientIOBench extends Benchmark<ClientIOTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(StressClientIOBench.class);

  @ParametersDelegate
  private ClientIOParameters mParameters = new ClientIOParameters();

  /** Cached FS instances. */
  private FileSystem[] mCachedFs;
    /** Set to true after the first barrier is passed. */
  private volatile boolean mStartBarrierPassed = false;

  /**
   * Creates instance.
   */
  public StressClientIOBench() {
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new StressClientIOBench());
  }

  @Override
  public void prepare() throws Exception {
    if (mBaseParameters.mCluster && mBaseParameters.mClusterLimit != 1) {
      throw new IllegalArgumentException(String.format(
          "%s is a single-node client IO stress test, so it cannot be run in cluster mode without"
              + " flag '%s 1'.", this.getClass().getName(), BaseParameters.CLUSTER_LIMIT_FLAG));
    }
    if (FormatUtils.parseSpaceSize(mParameters.mFileSize) < FormatUtils
        .parseSpaceSize(mParameters.mBufferSize)) {
      throw new IllegalArgumentException(String
          .format("File size (%s) must be larger than buffer size (%s)", mParameters.mFileSize,
              mParameters.mBufferSize));
    }
    if (mParameters.mOperation == ClientIOOperation.WRITE) {
      LOG.warn("Cannot write repeatedly, so warmup is not possible. Setting warmup to 0s.");
      mParameters.mWarmup = "0s";
    }
    if (!mBaseParameters.mDistributed) {
      // set hdfs conf for preparation client
      Configuration hdfsConf = new Configuration();
      hdfsConf.set(PropertyKey.Name.USER_FILE_DELETE_UNCHECKED, "true");
      hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
      FileSystem prepareFs = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);

      // initialize the base, for only the non-distributed task (the cluster launching task)
      Path path = new Path(mParameters.mBasePath);

      if (!ClientIOOperation.isRead(mParameters.mOperation)) {
        prepareFs.delete(path, true);
        prepareFs.mkdirs(path);
      }
    }

    ClientIOWritePolicy.setMaxWorkers(mParameters.mWriteNumWorkers);

    // set hdfs conf for all test clients
    Configuration hdfsConf = new Configuration();
    // do not cache these clients
    hdfsConf.set(
        String.format("fs.%s.impl.disable.cache", (new URI(mParameters.mBasePath)).getScheme()),
        "true");
    hdfsConf.set(PropertyKey.Name.USER_BLOCK_WRITE_LOCATION_POLICY,
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
  public ClientIOTaskResult runLocal() throws Exception {
    List<Integer> threadCounts = new ArrayList<>(mParameters.mThreads);
    threadCounts.sort(Comparator.comparingInt(i -> i));

    ClientIOTaskResult taskResult = new ClientIOTaskResult();
    taskResult.setBaseParameters(mBaseParameters);
    taskResult.setParameters(mParameters);
    for (Integer numThreads : threadCounts) {
      ClientIOTaskResult.ThreadCountResult threadCountResult = runForThreadCount(numThreads);
      if (!mBaseParameters.mProfileAgent.isEmpty()) {
        taskResult.putTimeToFirstBytePerThread(numThreads,
            addAdditionalResult(
                threadCountResult.getRecordStartMs(),
                threadCountResult.getEndMs()));
      }
      taskResult.addThreadCountResults(numThreads, threadCountResult);
    }

    return taskResult;
  }

  private ClientIOTaskResult.ThreadCountResult runForThreadCount(int numThreads) throws Exception {
    LOG.info("Running benchmark for thread count: " + numThreads);
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", numThreads).create();

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (startMs == BaseParameters.UNDEFINED_START_MS || mStartBarrierPassed) {
      // if the barrier was already passed, then overwrite the start time
      startMs = CommonUtils.getCurrentMs() + 10000;
    }
    long endMs = startMs + warmupMs + durationMs;
    BenchContext context = new BenchContext(startMs, endMs);

    List<Callable<Void>> callables = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      callables.add(new BenchThread(context, mCachedFs[i % mCachedFs.length], i));
    }
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);

    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);

    ClientIOTaskResult.ThreadCountResult result = context.getResult();

    LOG.info(String.format("thread count: %d, errors: %d, IO throughput (MB/s): %f", numThreads,
        result.getErrors().size(), result.getIOMBps()));

    return result;
  }

  /**
   * Read the log file from java agent log file.
   *
   * @param startMs start time for profiling
   * @param endMs end time for profiling
   * @return TimeToFirstByteStatistics
   * @throws IOException
   */
  @SuppressFBWarnings(value = "DMI_HARDCODED_ABSOLUTE_FILENAME")
  public synchronized Map<String, SummaryStatistics> addAdditionalResult(
      long startMs, long endMs) throws IOException {
    Map<String, SummaryStatistics> summaryStatistics = new HashMap<>();

    Map<String, MethodStatistics> nameStatistics =
        processMethodProfiles(startMs, endMs, profileInput -> {
          if (profileInput.getIsttfb()) {
            return profileInput.getMethod();
          }
          return null;
        });
    if (!nameStatistics.isEmpty()) {
      for (Map.Entry<String, MethodStatistics> entry : nameStatistics.entrySet()) {
        summaryStatistics.put(
            entry.getKey(), toSummaryStatistics(entry.getValue()));
      }
    }

    return summaryStatistics;
  }

  /**
   * Converts this class to {@link SummaryStatistics}.
   *
   * @param methodStatistics the method statistics
   * @return new SummaryStatistics
   */
  private SummaryStatistics toSummaryStatistics(MethodStatistics methodStatistics) {
    float[] responseTimePercentile = new float[101];
    for (int i = 0; i <= 100; i++) {
      responseTimePercentile[i] =
          (float) methodStatistics.getTimeNs().getValueAtPercentile(i) / Constants.MS_NANO;
    }

    float[] responseTime99Percentile = new float[StressConstants.TIME_99_COUNT];
    for (int i = 0; i < responseTime99Percentile.length; i++) {
      responseTime99Percentile[i] = (float) methodStatistics.getTimeNs()
          .getValueAtPercentile(100.0 - 1.0 / (Math.pow(10.0, i))) / Constants.MS_NANO;
    }

    float[] maxResponseTimesMs = new float[StressConstants.MAX_TIME_COUNT];
    Arrays.fill(maxResponseTimesMs, -1);
    for (int i = 0; i < methodStatistics.getMaxTimeNs().length; i++) {
      maxResponseTimesMs[i] = (float) methodStatistics.getMaxTimeNs()[i] / Constants.MS_NANO;
    }

    return new SummaryStatistics(methodStatistics.getNumSuccess(),
        responseTimePercentile,
        responseTime99Percentile, maxResponseTimesMs);
  }

  private final class BenchContext {
    private final long mStartMs;
    private final long mEndMs;

    /** The results. Access must be synchronized for thread safety. */
    private ClientIOTaskResult.ThreadCountResult mThreadCountResult;

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

    public synchronized void mergeThreadResult(ClientIOTaskResult.ThreadCountResult threadResult) {
      if (mThreadCountResult == null) {
        mThreadCountResult = threadResult;
      } else {
        try {
          mThreadCountResult.merge(threadResult);
        } catch (Exception e) {
          mThreadCountResult.addErrorMessage(e.getMessage());
        }
      }
    }

    public synchronized ClientIOTaskResult.ThreadCountResult getResult() {
      return mThreadCountResult;
    }
  }

  private final class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final Path mFilePath;
    private final FileSystem mFs;
    private final byte[] mBuffer;
    private final ByteBuffer mByteBuffer;
    private final int mThreadId;
    private final long mFileSize;
    private final long mMaxOffset;
    private final Iterator<Long> mLongs;
    private final long mBlockSize;

    private final ClientIOTaskResult.ThreadCountResult mThreadCountResult =
        new ClientIOTaskResult.ThreadCountResult();

    private FSDataInputStream mInStream = null;
    private FSDataOutputStream mOutStream = null;
    private long mCurrentOffset;

    private BenchThread(BenchContext context, FileSystem fs, int threadId) {
      mContext = context;
      mThreadId = threadId;

      int fileId = mThreadId;
      if (mParameters.mReadSameFile) {
        // all threads read the first file
        fileId = 0;
      }
      mFilePath = new Path(mParameters.mBasePath, "data-" + fileId);

      mFs = fs;

      mBuffer = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mBufferSize)];
      Arrays.fill(mBuffer, (byte) 'A');
      mByteBuffer = ByteBuffer.wrap(mBuffer);

      mFileSize = FormatUtils.parseSpaceSize(mParameters.mFileSize);
      mCurrentOffset = mFileSize;
      mMaxOffset = mFileSize - mBuffer.length;
      mBlockSize = FormatUtils.parseSpaceSize(mParameters.mBlockSize);

      mLongs = new Random().longs(0, mMaxOffset).iterator();
    }

    @Override
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
        LOG.error(Thread.currentThread().getName() + ": failed", e);
        mThreadCountResult.addErrorMessage(e.getMessage());
      } finally {
        closeInStream();
      }

      // Update thread count result
      mThreadCountResult.setEndMs(CommonUtils.getCurrentMs());
      mContext.mergeThreadResult(mThreadCountResult);

      return null;
    }

    private void runInternal() throws Exception {
      // When to start recording measurements
      long recordMs = mContext.getStartMs() + FormatUtils.parseTimeSize(mParameters.mWarmup);
      mThreadCountResult.setRecordStartMs(recordMs);
      boolean isRead = ClientIOOperation.isRead(mParameters.mOperation);

      long waitMs = mContext.getStartMs() - CommonUtils.getCurrentMs();
      if (waitMs < 0) {
        throw new IllegalStateException(String.format(
            "Thread missed barrier. Set the start time to a later time. start: %d current: %d",
            mContext.getStartMs(), CommonUtils.getCurrentMs()));
      }
      CommonUtils.sleepMs(waitMs);
      mStartBarrierPassed = true;

      while (!Thread.currentThread().isInterrupted() && (!isRead
          || CommonUtils.getCurrentMs() < mContext.getEndMs())) {
        int ioBytes = applyOperation();

        long currentMs = CommonUtils.getCurrentMs();
        // Start recording after the warmup
        if (currentMs > recordMs) {
          if (ioBytes > 0) {
            mThreadCountResult.incrementIOBytes(ioBytes);
          }
          if (mParameters.mOperation == ClientIOOperation.WRITE && ioBytes < 0) {
            // done writing. done with the thread.
            break;
          }
        }
      }
    }

    private int applyOperation() throws IOException {
      if (ClientIOOperation.isRead(mParameters.mOperation)) {
        if (mInStream == null) {
          mInStream = mFs.open(mFilePath);
        }
        if (mParameters.mReadRandom) {
          mCurrentOffset = mLongs.next();
          if (!ClientIOOperation.isPosRead(mParameters.mOperation)) {
            // must seek if not a positioned read
            mInStream.seek(mCurrentOffset);
          }
        } else {
          mCurrentOffset += mBuffer.length;
          if (mCurrentOffset > mMaxOffset) {
            mCurrentOffset = 0;
          }
        }
      }

      switch (mParameters.mOperation) {
        case READ_ARRAY: {
          int bytesRead = mInStream.read(mBuffer);
          if (bytesRead < 0) {
            closeInStream();
            mInStream = mFs.open(mFilePath);
          }
          return bytesRead;
        }
        case READ_BYTE_BUFFER: {
          int bytesRead = mInStream.read(mByteBuffer);
          if (bytesRead < 0) {
            closeInStream();
            mInStream = mFs.open(mFilePath);
          }
          return bytesRead;
        }
        case READ_FULLY: {
          int toRead = Math.min(mBuffer.length, (int) (mFileSize - mInStream.getPos()));
          mInStream.readFully(mBuffer, 0, toRead);
          if (mInStream.getPos() == mFileSize) {
            closeInStream();
            mInStream = mFs.open(mFilePath);
          }
          return toRead;
        }
        case POS_READ: {
          return mInStream.read(mCurrentOffset, mBuffer, 0, mBuffer.length);
        }
        case POS_READ_FULLY: {
          mInStream.readFully(mCurrentOffset, mBuffer, 0, mBuffer.length);
          return mBuffer.length;
        }
        case WRITE: {
          if (mOutStream == null) {
            mOutStream = mFs.create(mFilePath, false, mBuffer.length, (short) 1, mBlockSize);
          }
          int bytesToWrite = (int) Math.min(mFileSize - mOutStream.getPos(), mBuffer.length);
          if (bytesToWrite == 0) {
            mOutStream.close();
            return -1;
          }
          mOutStream.write(mBuffer, 0, bytesToWrite);
          return bytesToWrite;
        }
        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }

    private void closeInStream() {
      try {
        if (mInStream != null) {
          mInStream.close();
        }
      } catch (IOException e) {
        mThreadCountResult.addErrorMessage(e.getMessage());
      } finally {
        mInStream = null;
      }
    }
  }
}
