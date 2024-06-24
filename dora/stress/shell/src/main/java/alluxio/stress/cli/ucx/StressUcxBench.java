package alluxio.stress.cli.ucx;

import static alluxio.Constants.MB;
import static alluxio.Constants.SECOND_NANO;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.conf.PropertyKey;
import alluxio.grpc.LoadFileRequest;
import alluxio.grpc.LoadFileResponse;
import alluxio.grpc.UfsReadOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.stress.cli.AbstractStressBench;
import alluxio.stress.cli.worker.StressWorkerBench;
import alluxio.stress.worker.WorkerBenchCoarseDataPoint;
import alluxio.stress.worker.WorkerBenchDataPoint;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.stress.worker.WorkerBenchTaskResult;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.logging.SamplingLogger;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.ucx.UcpServer;
import alluxio.worker.ucx.UcxDataReader;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.fs.Path;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class StressUcxBench extends StressWorkerBench {
  private static final Logger LOG = LoggerFactory.getLogger(StressUcxBench.class);
  private static final Logger SAMPLING_LOG =
      new SamplingLogger(LoggerFactory.getLogger(StressUcxBench.class),
          10L * Constants.SECOND_MS);
  private String[] mFilePaths;
  private InetSocketAddress mUcpServerAddr;
  private final long mFileSize = 1024 * 1024l;

  public StressUcxBench () {
    super();
  }

  @Override
  public String getBenchDescription() {
    return null;
  }

  public static void main(String[] args) {
    mainInternal(args, new StressUcxBench());
  }

  @Override
  public void prepare() throws Exception {
    // cache pages on cachemanager
    int numOfFiles = mParameters.mThreads;
    mFilePaths = new String[numOfFiles];
    mUcpServerAddr = new InetSocketAddress(mParameters.mUcpHost, mParameters.mUcpPort);
    LOG.info("Use {} as target ucp server", mUcpServerAddr);
    for (int i = 0; i < numOfFiles; i++) {
        mFilePaths[i] = String.format("%s%d", mParameters.mBasePath, i+1);
    }
  }

  @Override
  public void validateParams() throws Exception {
    LOG.info("No op in validateParams");
  }

  @Override
  public WorkerBenchTaskResult runLocal() throws Exception {
    // If running in this one process, do all the work
    // Otherwise, calculate its own part and only do that
    int startFileIndex = 0;
    int endFileIndex = mParameters.mThreads;
    LOG.info("This is running in the command line process. Read all {} files with {} threads.",
        endFileIndex, mParameters.mThreads);

    ExecutorService service =
        ExecutorServiceFactories.fixedThreadPool("bench-thread", mParameters.mThreads).create();

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    long warmupMs = FormatUtils.parseTimeSize(mParameters.mWarmup);
    long startMs = mBaseParameters.mStartMs;
    if (startMs <= 0) {
      // automatically sets it to be 30sec later
      startMs = CommonUtils.getCurrentMs() + 10 * 1000;
    }
    long endMs = startMs + warmupMs + durationMs;
    String datePattern = alluxio.conf.Configuration.global()
        .getString(PropertyKey.USER_DATE_FORMAT_PATTERN);
    SAMPLING_LOG.info("StressWorkerBench has start={}, warmup={}ms, end={}",
        CommonUtils.convertMsToDate(startMs, datePattern),
        warmupMs,
        CommonUtils.convertMsToDate(endMs, datePattern));
    BenchContext context = new BenchContext(startMs, endMs);

    List<Callable<Void>> callables = new ArrayList<>(mParameters.mThreads);
    // Each thread will have one file created for it
    // And that thread keeps reading one same file over and over
    for (int threadIndex = 0; threadIndex < mParameters.mThreads; threadIndex++) {
      int fileIndex = startFileIndex + threadIndex;
      LOG.info("Thread {} reads file {} path {}", threadIndex, fileIndex, mFilePaths[fileIndex]);
      callables.add(new BenchThread(context, fileIndex));
    }
    service.invokeAll(callables, FormatUtils.parseTimeSize(mBaseParameters.mBenchTimeout),
        TimeUnit.MILLISECONDS);

    Thread.sleep(20000);
    service.shutdownNow();
    service.awaitTermination(30, TimeUnit.SECONDS);
    return context.getResult();
  }

  private static final class BenchContext {
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
      LOG.info("Merging result:{}", threadResult);
      if (mResult == null) {
        mResult = new WorkerBenchTaskResult();
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
    private ByteBuffer mBuffer;
    private int mBufferSize;
    private WorkerBenchTaskResult mResult;
    private int mTargetFileIndex;
    private final boolean mIsRandomRead;
    private final long mRandomMax;
    private final long mRandomMin;
    private final BenchContext mContext;
    private UcxDataReader mReader;

    private class ApplyOperationOutput {
      public final long mBytesRead;
      public final long mDuration;

      public ApplyOperationOutput(long bytesRead, long duration) {
        mBytesRead = bytesRead;
        mDuration = duration;
      }
    }

    private BenchThread(BenchContext context, int targetFileIndex) {
      mContext = context;
      mBufferSize = (int) FormatUtils.parseSpaceSize(mParameters.mBufferSize);
      mBuffer = ByteBuffer.allocateDirect(mBufferSize);
      mTargetFileIndex = targetFileIndex;
      mResult = new WorkerBenchTaskResult();
      mResult.setParameters(mParameters);
      mResult.setBaseParameters(mBaseParameters);
      mIsRandomRead = mParameters.mIsRandom;
      mRandomMin =  FormatUtils.parseSpaceSize(mParameters.mRandomMinReadLength);
      mRandomMax =  FormatUtils.parseSpaceSize(mParameters.mRandomMaxReadLength);
    }

    @Override
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
        LOG.error(Thread.currentThread().getName() + ": failed", e);
        mResult.addErrorMessage(e.getMessage());
      } finally {
        mReader.close();
      }

      // Update local thread end time
      mResult.setEndMs(CommonUtils.getCurrentMs());

      // merge local thread result with full result
      mContext.mergeThreadResult(mResult);
      return null;
    }


    public void runInternal() throws Exception {
      String ufsFilePath = mFilePaths[mTargetFileIndex];
      Protocol.OpenUfsBlockOptions openUfsBlockOptions =
          Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(ufsFilePath)
              .setOffsetInFile(0).setBlockSize(mFileSize)
              .setNoCache(true)
              .setMountId(0)
              .build();
      UcpContext ucpContext = new UcpContext(new UcpParams()
          .requestStreamFeature()
          .requestTagFeature()
          .requestRmaFeature()
          .requestWakeupFeature());
      UcpWorker ucpWorker = ucpContext.newWorker(new UcpWorkerParams()
          .requestWakeupRMA()
          .requestThreadSafety());

      Protocol.ReadRequestRMA.Builder requestRMABuilder = Protocol.ReadRequestRMA.newBuilder()
          .setOpenUfsBlockOptions(openUfsBlockOptions);
      mReader = new UcxDataReader(mUcpServerAddr, ucpWorker, null, requestRMABuilder);

      // When to start recording measurements
      long recordMs = mContext.getStartMs() + FormatUtils.parseTimeSize(mParameters.mWarmup);
      mResult.setRecordStartMs(recordMs);

      long waitMs = mContext.getStartMs() - CommonUtils.getCurrentMs();
      if (waitMs < 0) {
        throw new IllegalStateException(String.format(
            "Thread missed barrier. Increase the start delay. start: %d current: %d",
            mContext.getStartMs(), CommonUtils.getCurrentMs()));
      }
      String dateFormat = alluxio.conf.Configuration.global()
          .getString(PropertyKey.USER_DATE_FORMAT_PATTERN);
      SAMPLING_LOG.info("Scheduled to start at {}, wait {}ms for the scheduled start",
          CommonUtils.convertMsToDate(mContext.getStartMs(), dateFormat),
          waitMs);
      CommonUtils.sleepMs(waitMs);
      SAMPLING_LOG.info("Test started and recording will be started after the warm up at {}",
          CommonUtils.convertMsToDate(recordMs, dateFormat));

      WorkerBenchCoarseDataPoint dp = new WorkerBenchCoarseDataPoint(
          Long.parseLong("0"),
          Thread.currentThread().getId());
      WorkerBenchDataPoint slice = new WorkerBenchDataPoint();
      List<Long> throughputList = new ArrayList<>();
      long lastSlice = 0;

      while (!Thread.currentThread().isInterrupted()
          && CommonUtils.getCurrentMs() < mContext.getEndMs()) {
        // Keep reading the same file
        long startMs = CommonUtils.getCurrentMs() - recordMs;
        BenchThread.ApplyOperationOutput output = applyOperation();
        if (startMs > 0) {
          if (output.mBytesRead > 0) {
            mResult.setIOBytes(mResult.getIOBytes() + output.mBytesRead);
            slice.mCount += 1;
            slice.mIOBytes += output.mBytesRead;
            if (output.mDuration > 0) {
              // throughput unit: MB/s
              // max file size allowed: 9223372036B (8.5GB)
              throughputList.add(output.mBytesRead * SECOND_NANO / (MB * output.mDuration));
            } else if (output.mDuration == 0) {
              // if duration is 0ns, treat is as 1ns
              throughputList.add(output.mBytesRead * SECOND_NANO / MB);
              SAMPLING_LOG.warn("Thread for file {} read operation finished in 0ns",
                  mFilePaths[mTargetFileIndex]);
            } else {
              // if duration is negative, throw an exception
              throw new IllegalStateException(String.format(
                  "Negative duration for file read: %d", output.mDuration));
            }
            int currentSlice = (int) (startMs
                / FormatUtils.parseTimeSize(mParameters.mSliceSize));
            while (currentSlice > lastSlice) {
              dp.addDataPoint(slice);
              slice = new WorkerBenchDataPoint();
              lastSlice++;
            }
          } else {
            LOG.warn("Thread for file {} read 0 bytes from I/O", mFilePaths[mTargetFileIndex]);
          }
        } else {
          SAMPLING_LOG.info("Ignored record during warmup: {} bytes", output.mBytesRead);
        }
      }

      int finalSlice = (int) (FormatUtils.parseTimeSize(mParameters.mDuration)
          / FormatUtils.parseTimeSize(mParameters.mSliceSize));
      while (finalSlice > lastSlice) {
        dp.addDataPoint(slice);
        slice = new WorkerBenchDataPoint();
        lastSlice++;
      }
      dp.setThroughput(throughputList);
      mResult.addDataPoint(dp);
    }


    /**
     * Read the file by the offset and length based on the given index.
     * @return the actual red byte number
     */
    private BenchThread.ApplyOperationOutput applyOperation() throws IOException {
      long startReadNs = System.nanoTime();
      mReader.acquireServerConn();
      // sequential
      int bytesRead = 0;
      while (bytesRead < mFileSize) {
        mBuffer.clear();
        int actualReadLength = mReader.read(bytesRead, mBuffer, mBufferSize);
        if (actualReadLength <= 0) {
          break;
        } else {
          bytesRead += actualReadLength;
        }
      }

      // We use the nanoTime only to calculate elapsed time
      long afterReadNs = System.nanoTime();
      return new BenchThread.ApplyOperationOutput(bytesRead, afterReadNs - startReadNs);
    }
  }
}
