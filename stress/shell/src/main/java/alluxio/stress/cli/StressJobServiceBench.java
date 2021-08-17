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

import alluxio.ClientContext;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.PropertyKey;
import alluxio.job.JobConfig;
import alluxio.stress.BaseParameters;
import alluxio.stress.StressConstants;
import alluxio.stress.jobservice.JobServiceBenchParameters;
import alluxio.stress.jobservice.JobServiceBenchTaskResult;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.worker.job.JobMasterClientContext;

import com.beust.jcommander.ParametersDelegate;
import org.HdrHistogram.Histogram;
import org.apache.hadoop.conf.Configuration;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Job Service stress bench
 */
public class StressJobServiceBench extends Benchmark<JobServiceBenchTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(StressJobServiceBench.class);

  @ParametersDelegate
  private JobServiceBenchParameters mParameters = new JobServiceBenchParameters();

  private JobMasterClient[] mJobMasterClients;

  /**
   * Creates instance.
   */
  public StressJobServiceBench() {
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new StressJobServiceBench());
  }

  @Override
  public void prepare() throws Exception {

    if (!mBaseParameters.mDistributed) {
      // set hdfs conf for preparation client
      Configuration hdfsConf = new Configuration();
      // force delete, create dirs through to UFS
      hdfsConf.set(PropertyKey.Name.USER_FILE_DELETE_UNCHECKED, "true");
      hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");
      // more threads for parallel deletes for cleanup
      hdfsConf.set(PropertyKey.Name.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX, "256");
      FileSystem prepareFs = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);

      // initialize the base, for only the non-distributed task (the cluster launching task)
      Path path = new Path(mParameters.mBasePath);

      // the base path depends on the operation
      Path basePath = new Path(path, "dirs");

        // these are read operations. the directory must exist
        if (!prepareFs.exists(basePath)) {
          throw new IllegalStateException(String
              .format("base path (%s) must exist for operation (%s)", basePath,
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
    mJobMasterClients = new JobMasterClient[mParameters.mClients];
    for (int i = 0; i < mParameters.mClients; i++) {
      mJobMasterClients[i] = JobMasterClient.Factory.create(
          JobMasterClientContext.newBuilder(ClientContext.create()).build());
    }
    //create files for given parameter
//    fs.mkdirs();
//    fs.create();
//    byte[] fileData = new byte[(int) FormatUtils.parseSpaceSize(mParameters.mCreateFileSize)];
//    Arrays.fill(fileData, (byte) 0x7A);

  }



  @Override public String getBenchDescription() {
    return "";
  }

  @Override
  public JobServiceBenchTaskResult runLocal() throws Exception {
    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mThreads).create();




    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (mBaseParameters.mStartMs == BaseParameters.UNDEFINED_START_MS) {
      startMs = CommonUtils.getCurrentMs() + 1000;
    }
    long endMs = startMs + warmupMs + durationMs;
    JobConfig config = null;
    BenchContext context = new BenchContext(config,startMs, endMs);

    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
    for (int i = 0; i < mParameters.mThreads; i++) {
      callables.add(new BenchThread(context, mJobMasterClients[i % mJobMasterClients.length]));
    }
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);

    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);
    for (int i = 0; i < mParameters.mClients; i++) {
      mJobMasterClients[i].close();
    }

    return context.getResult();
  }

  private final class BenchContext {
    private final JobConfig mConfig;
    private final long mStartMs;
    private final long mEndMs;
    private final AtomicLong mCounter;

    /** The results. Access must be synchronized for thread safety. */
    private JobServiceBenchTaskResult mResult;

    public BenchContext(JobConfig config, long startMs, long endMs) {
      mConfig = config;
      mStartMs = startMs;
      mEndMs = endMs;
      mCounter = new AtomicLong();
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

    public synchronized void mergeThreadResult(JobServiceBenchTaskResult threadResult) {
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

    public synchronized JobServiceBenchTaskResult getResult() {
      return mResult;
    }
  }

  private final class BenchThread implements Callable<Void> {
    private final BenchContext mContext;
    private final Histogram mResponseTimeNs;
    private final JobMasterClient mJobMasterClient;

    private final JobServiceBenchTaskResult mResult = new JobServiceBenchTaskResult();

    private BenchThread(BenchContext context, JobMasterClient client) {
      mContext = context;
      mResponseTimeNs = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
          StressConstants.TIME_HISTOGRAM_PRECISION);
      mJobMasterClient = client;
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

      long bucketSize = (mContext.getEndMs() - recordMs) / StressConstants.MAX_TIME_COUNT;

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

      switch (mParameters.mOperation) {
        case DISTRIBUTED_LOAD:
          // send distributed load task to job service and wait for result

          mJobMasterClient.run(mContext.mConfig);

          break;
        default:
          throw new IllegalStateException("Unknown operation: " + mParameters.mOperation);
      }
    }
  }
}
