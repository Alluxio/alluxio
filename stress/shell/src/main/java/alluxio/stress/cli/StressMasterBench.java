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

import alluxio.conf.PropertyKey;
import alluxio.job.plan.PlanConfig;
import alluxio.stress.BaseParameters;
import alluxio.stress.job.StressBenchConfig;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.stress.master.MasterBenchTaskResult;
import alluxio.stress.master.MasterBenchTaskResultStatistics;
import alluxio.stress.master.Operation;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.HdrHistogram.Histogram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Single node stress test.
 */
public class StressMasterBench extends Benchmark<MasterBenchTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(StressMasterBench.class);

  private static final String AGENT_OUTPUT_PATH = "/tmp/stress_master.log";

  @ParametersDelegate
  private MasterBenchParameters mParameters = new MasterBenchParameters();

  private byte[] mFiledata;
  private FileSystem[] mCachedFs;

  /**
   * Creates instance.
   */
  public StressMasterBench() {
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new StressMasterBench());
  }

  @Override
  public void prepare() throws Exception {
    if (mParameters.mFixedCount <= 0) {
      throw new IllegalStateException(
          "fixed count must be > 0. fixedCount: " + mParameters.mFixedCount);
    }

    if (!mParameters.mProfileAgent.isEmpty()) {
      mBaseParameters.mJavaOpts.add("-javaagent:" + mParameters.mProfileAgent
          + "=" + AGENT_OUTPUT_PATH);
    }

    if (!mBaseParameters.mDistributed) {
      // set hdfs conf for preparation client
      Configuration hdfsConf = new Configuration();
      // force delete, create dirs through to UFS
      hdfsConf.set(PropertyKey.Name.USER_FILE_DELETE_UNCHECKED, "true");
      hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");
      FileSystem prepareFs = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);

      // initialize the base, for only the non-distributed task (the cluster launching task)
      Path path = new Path(mParameters.mBasePath);

      // the base path depends on the operation
      Path basePath;
      if (mParameters.mOperation == Operation.CreateDir) {
        basePath = new Path(path, "dirs");
      } else {
        basePath = new Path(path, "files");
      }

      if (mParameters.mOperation == Operation.CreateFile
          || mParameters.mOperation == Operation.CreateDir) {
        prepareFs.delete(basePath, true);
        prepareFs.mkdirs(basePath);
      } else {
        // these are read operations. the directory must exist
        if (!prepareFs.exists(basePath)) {
          throw new IllegalStateException(String
              .format("base path (%s) must exist for operation (%s)", basePath,
                  mParameters.mOperation));
        }
      }
      if (!prepareFs.isDirectory(basePath)) {
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
    mCachedFs = new FileSystem[mParameters.mClients];
    for (int i = 0; i < mCachedFs.length; i++) {
      mCachedFs[i] = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);
    }
  }

  @Override
  public MasterBenchTaskResult runLocal() throws Exception {
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mThreads).create();

    RateLimiter rateLimiter = RateLimiter.create(mParameters.mTargetThroughput);

    mFiledata = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mCreateFileSize)];
    Arrays.fill(mFiledata, (byte) 0x7A);

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (mBaseParameters.mStartMs == BaseParameters.UNDEFINED_START_MS) {
      startMs = CommonUtils.getCurrentMs() + 1000;
    }
    long endMs = startMs + warmupMs + durationMs;
    BenchContext context = new BenchContext(rateLimiter, startMs, endMs);

    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
    for (int i = 0; i < mParameters.mThreads; i++) {
      callables.add(new BenchThread(context, mCachedFs[i % mCachedFs.length]));
    }
    service.invokeAll(callables, 10, TimeUnit.MINUTES);

    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);

    if (!mParameters.mProfileAgent.isEmpty()) {
      context.addAdditionalResult();
    }

    return context.getResult();
  }

  private final class BenchContext {
    private final RateLimiter mRateLimiter;
    private final long mStartMs;
    private final long mEndMs;
    private final AtomicLong mCounter;

    /** The results. Access must be synchronized for thread safety. */
    private MasterBenchTaskResult mResult;

    public BenchContext(RateLimiter rateLimiter, long startMs, long endMs) {
      mRateLimiter = rateLimiter;
      mStartMs = startMs;
      mEndMs = endMs;
      mCounter = new AtomicLong();
    }

    public RateLimiter getRateLimiter() {
      return mRateLimiter;
    }

    public long getStartMs() {
      return mStartMs;
    }

    public long getEndMs() {
      return mEndMs;
    }

    public AtomicLong getCounter() {
      return mCounter;
    }

    public synchronized void mergeThreadResult(MasterBenchTaskResult threadResult) {
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

    @SuppressFBWarnings(value = "DMI_HARDCODED_ABSOLUTE_FILENAME")
    public synchronized void addAdditionalResult() throws IOException {
      if (mResult == null) {
        return;
      }

      Map<String, PartialResultStatistic> methodNameToHistogram = new HashMap<>();

      try (final BufferedReader reader = new BufferedReader(new FileReader(AGENT_OUTPUT_PATH))) {
        String line;

        long bucketSize = (mResult.getEndMs() - mResult.getRecordStartMs())
            / MasterBenchTaskResultStatistics.MAX_RESPONSE_TIME_COUNT;

        final ObjectMapper objectMapper = new ObjectMapper();
        while ((line = reader.readLine()) != null) {
          final Map<String, Object> lineMap;
          try {
            lineMap = objectMapper.readValue(line, Map.class);
          } catch (JsonParseException e) {
            // skip the last line of a not completed file
            break;
          }

          final String type = (String) lineMap.get("type");
          final String methodName = (String) lineMap.get("methodName");
          final Long timestamp = (Long) lineMap.get("timestamp");
          final Integer duration = (Integer) lineMap.get("duration");

          if (timestamp <= mResult.getRecordStartMs()) {
            continue;
          }

          if (type != null && methodName != null && duration != null) {
            if (!methodNameToHistogram.containsKey(methodName)) {
              methodNameToHistogram.put(methodName, new PartialResultStatistic());
            }

            final PartialResultStatistic statistic = methodNameToHistogram.get(methodName);
            statistic.mResponseTimeNs.recordValue(duration);
            statistic.mNumSuccess += 1;

            int bucket =
                Math.min(statistic.mMaxResponseTimeNs.length - 1,
                    (int) ((timestamp - mResult.getRecordStartMs()) / bucketSize));
            if (duration > statistic.mMaxResponseTimeNs[bucket]) {
              statistic.mMaxResponseTimeNs[bucket] = duration;
            }
          }
        }
      }

      for (Map.Entry<String, PartialResultStatistic> entry : methodNameToHistogram.entrySet()) {
        final MasterBenchTaskResultStatistics stats = new MasterBenchTaskResultStatistics();
        stats.encodeResponseTimeNsRaw(entry.getValue().mResponseTimeNs);
        stats.mNumSuccess = entry.getValue().mNumSuccess;
        stats.mMaxResponseTimeNs = entry.getValue().mMaxResponseTimeNs;
        mResult.putStatisticsForMethod(entry.getKey(), stats);
      }
    }

    public synchronized MasterBenchTaskResult getResult() {
      return mResult;
    }
  }

  private final class PartialResultStatistic {
    private Histogram mResponseTimeNs;
    private int mNumSuccess;
    private long[] mMaxResponseTimeNs;

    public PartialResultStatistic() {
      mNumSuccess = 0;
      mResponseTimeNs = new Histogram(
          MasterBenchTaskResultStatistics.RESPONSE_TIME_HISTOGRAM_MAX,
          MasterBenchTaskResultStatistics.RESPONSE_TIME_HISTOGRAM_PRECISION);
      mMaxResponseTimeNs = new long[MasterBenchTaskResultStatistics.MAX_RESPONSE_TIME_COUNT];
      Arrays.fill(mMaxResponseTimeNs, -1);
    }
  }

  private final class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final Histogram mResponseTimeNs;
    private final Path mBasePath;
    private final Path mFixedBasePath;
    private final FileSystem mFs;

    private final MasterBenchTaskResult mResult = new MasterBenchTaskResult();

    private BenchThread(BenchContext context, FileSystem fs) {
      mContext = context;
      mResponseTimeNs = new Histogram(MasterBenchTaskResultStatistics.RESPONSE_TIME_HISTOGRAM_MAX,
          MasterBenchTaskResultStatistics.RESPONSE_TIME_HISTOGRAM_PRECISION);
      if (mParameters.mOperation == Operation.CreateDir) {
        mBasePath =
            new Path(PathUtils.concatPath(mParameters.mBasePath, "dirs", mBaseParameters.mId));
      } else {
        mBasePath =
            new Path(PathUtils.concatPath(mParameters.mBasePath, "files", mBaseParameters.mId));
      }
      mFixedBasePath = new Path(mBasePath, "fixed");
      mFs = fs;
    }

    @Override
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
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

    private void runInternal() throws Exception {
      // When to start recording measurements
      long recordMs = mContext.getStartMs() + FormatUtils.parseTimeSize(mParameters.mWarmup);
      mResult.setRecordStartMs(recordMs);

      boolean useStopCount = mParameters.mStopCount != MasterBenchParameters.STOP_COUNT_INVALID;

      long bucketSize = (mContext.getEndMs() - recordMs)
          / MasterBenchTaskResultStatistics.MAX_RESPONSE_TIME_COUNT;

      long waitMs = mContext.getStartMs() - CommonUtils.getCurrentMs();
      if (waitMs < 0) {
        throw new IllegalStateException(String.format(
            "Thread missed barrier. Set the start time to a later time. start: %d current: %d",
            mContext.getStartMs(), CommonUtils.getCurrentMs()));
      }
      CommonUtils.sleepMs(waitMs);

      while (!Thread.currentThread().isInterrupted()
          && ((!useStopCount && CommonUtils.getCurrentMs() < mContext.getEndMs())
              || (useStopCount && mContext.mCounter.get() < mParameters.mStopCount))) {
        mContext.getRateLimiter().acquire();
        long startNs = System.nanoTime();
        applyOperation();
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

    private void applyOperation() throws IOException {
      long counter = mContext.getCounter().getAndIncrement();

      Path path;
      switch (mParameters.mOperation) {
        case CreateDir:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
          }
          mFs.mkdirs(path);
          break;
        case CreateFile:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
          }
          mFs.create(path).close();
          break;
        case GetBlockLocations:
          counter = counter % mParameters.mFixedCount;
          path = new Path(mFixedBasePath, Long.toString(counter));
          mFs.getFileBlockLocations(path, 0, 0);
          break;
        case GetFileStatus:
          counter = counter % mParameters.mFixedCount;
          path = new Path(mFixedBasePath, Long.toString(counter));
          mFs.getFileStatus(path);
          break;
        case ListDir:
          FileStatus[] files = mFs.listStatus(mFixedBasePath);
          if (files.length != mParameters.mFixedCount) {
            throw new IOException(String
                .format("listing `%s` expected %d files but got %d files", mFixedBasePath,
                    mParameters.mFixedCount, files.length));
          }
          break;
        case OpenFile:
          counter = counter % mParameters.mFixedCount;
          path = new Path(mFixedBasePath, Long.toString(counter));
          mFs.open(path).close();
          break;
        case RenameFile:
          if (counter < mParameters.mFixedCount) {
            path = new Path(mFixedBasePath, Long.toString(counter));
          } else {
            path = new Path(mBasePath, Long.toString(counter));
          }
          Path dst = new Path(path.toString() + "-renamed");
          if (!mFs.rename(path, dst)) {
            throw new IOException(String.format("Failed to rename (%s) to (%s)", path, dst));
          }
          break;
        case DeleteFile:
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
}
