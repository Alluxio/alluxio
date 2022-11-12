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
import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.PropertyKey;
import alluxio.grpc.WritePType;
import alluxio.stress.BaseParameters;
import alluxio.stress.cli.AbstractStressBench;
import alluxio.stress.cli.client.ClientIOWritePolicy;
import alluxio.stress.common.FileSystemParameters;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.stress.worker.WorkerBenchTaskResult;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.collect.ImmutableList;
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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Single node stress test.
 */
// TODO(jiacheng): avoid the implicit casts and @SuppressFBWarnings
public class StressWorkerBench extends AbstractStressBench<WorkerBenchTaskResult,
    WorkerBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(StressWorkerBench.class);

  private FileSystem[] mCachedFs;
  private Path[] mFilePaths;
  private Integer[] mOffsets;
  private Integer[] mLengths;

  /** generate random number in range [min, max] (include both min and max).*/
  private Integer randomNumInRange(Random rand, int min, int max) {
    return rand.nextInt(max - min + 1) + min;
  }

  /**
   * Creates instance.
   */
  public StressWorkerBench() {
    mParameters = new WorkerBenchParameters();
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new StressWorkerBench());
  }

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A benchmarking tool to measure the read performance of alluxio workers in the cluster",
        "The test will create one file and repeatedly read the created file to test the "
            + "performance",
        "",
        "Example:",
        "# This would create a 100MB file with block size of 16KB and then read the file "
            + "for 30s after 10s warmup",
        "$ bin/alluxio runClass alluxio.stress.cli.worker.StressWorkerBench --clients 1 "
            + "--base alluxio:///stress-worker-base --block-size 16k --file-size 100m "
            + "--warmup 10s --duration 30s --cluster\n"
    ));
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public void prepare() throws Exception {

    // Read and write to one worker
    ClientIOWritePolicy.setMaxWorkers(1);

    // initialize the base, for only the non-distributed task (the cluster launching task)
    Path path = new Path(mParameters.mBasePath);
    int fileSize = parseIntSpaceSize(mParameters.mFileSize);

    mFilePaths = new Path[mParameters.mNumFiles];
    // set random offsets and lengths if enabled
    mLengths = new Integer[mParameters.mNumFiles];
    mOffsets = new Integer[mParameters.mNumFiles];

    Random rand = new Random();
    if (mParameters.mIsRandom) {
      rand = new Random(mParameters.mRandomSeed);
    }
    for (int i = 0; i < mParameters.mNumFiles; i++) {
      Path filePath = new Path(path, "data" + i);
      mFilePaths[i] = filePath;
      if (mParameters.mIsRandom) {
        int randomMin = parseIntSpaceSize(mParameters.mRandomMinReadLength);
        int randomMax = parseIntSpaceSize(mParameters.mRandomMaxReadLength);
        mOffsets[i] = randomNumInRange(rand, 0, fileSize - 1 - randomMin);
        mLengths[i] = randomNumInRange(rand, randomMin,
            Integer.min(fileSize - mOffsets[i], randomMax));
      } else {
        mOffsets[i] = 0;
        mLengths[i] = fileSize;
      }
    }

    if (!mBaseParameters.mDistributed) {
      // set hdfs conf for preparation client
      Configuration hdfsConf = new Configuration();
      // force delete, create dirs through to UFS
      hdfsConf.set(PropertyKey.Name.USER_FILE_DELETE_UNCHECKED, "true");
      if (mParameters.mFree && WritePType.MUST_CACHE.name().equals(mParameters.mWriteType)) {
        throw new IllegalStateException(String.format("%s cannot be %s when %s option provided",
            FileSystemParameters.WRITE_TYPE_OPTION_NAME, WritePType.MUST_CACHE, "--free"));
      }
      hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, mParameters.mWriteType);
      hdfsConf.set(PropertyKey.Name.USER_BLOCK_WRITE_LOCATION_POLICY,
          ClientIOWritePolicy.class.getName());
      hdfsConf.set(PropertyKey.Name.USER_UFS_BLOCK_READ_LOCATION_POLICY,
          ClientIOWritePolicy.class.getName());
      FileSystem prepareFs = FileSystem.get(new URI(mParameters.mBasePath), hdfsConf);

      if (!mParameters.mSkipCreation) {
        prepareFs.delete(path, true);
        prepareFs.mkdirs(path);
        byte[] buffer = new byte[parseIntSpaceSize(mParameters.mBufferSize)];
        Arrays.fill(buffer, (byte) 'A');

        for (int i = 0; i < mParameters.mNumFiles; i++) {
          Path filePath = mFilePaths[i];
          try (FSDataOutputStream mOutStream = prepareFs
              .create(filePath, false, buffer.length, (short) 1,
                  FormatUtils.parseSpaceSize(mParameters.mBlockSize))) {
            while (true) {
              int bytesToWrite = (int) Math.min(fileSize - mOutStream.getPos(), buffer.length);
              if (bytesToWrite == 0) {
                break;
              }
              mOutStream.write(buffer, 0, bytesToWrite);
            }
          }
          if (mParameters.mFree && Constants.SCHEME.equals(filePath.toUri().getScheme())) {
            // free the alluxio file
            alluxio.client.file.FileSystem.Factory.get().free(new AlluxioURI(filePath.toString()));
            LOG.info("Freed file before reading: " + filePath);
          }
        }
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
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
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

  private static int parseIntSpaceSize(String value) {
    long longFileSize = FormatUtils.parseSpaceSize(value);
    if (longFileSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "space size value too large, must smaller than " + Integer.MAX_VALUE);
    }
    return (int) longFileSize;
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

    private byte[][] mBuffers;
    private final WorkerBenchTaskResult mResult;
    private final boolean mIsRandomReed;

    private final FSDataInputStream[] mInStreams = new FSDataInputStream[mFilePaths.length];

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    private BenchThread(BenchContext context, FileSystem fs) {
      mContext = context;
      mFs = fs;
      mBuffer = new byte[parseIntSpaceSize(mParameters.mBufferSize)];
      mResult = new WorkerBenchTaskResult();
      mResult.setParameters(mParameters);
      mResult.setBaseParameters(mBaseParameters);
      mIsRandomReed = mParameters.mIsRandom;
      if (mIsRandomReed) {
        mBuffers = new byte[mParameters.mNumFiles][];
        Random rand = new Random(mParameters.mRandomSeed);
        int minBufferSize = parseIntSpaceSize(mParameters.mRandomMinBufferSize);
        int randomMin = parseIntSpaceSize(mParameters.mRandomMinReadLength);
        minBufferSize = Math.min(minBufferSize, randomMin);
        for (int i = 0; i < mParameters.mNumFiles; i++) {
          mBuffers[i] = new byte[randomNumInRange(rand, minBufferSize, randomMin)];
        }
      }
    }

    @Override
    public Void call() {
      try {
        runInternal();
      } catch (Exception e) {
        LOG.error(Thread.currentThread().getName() + ": failed", e);
        mResult.addErrorMessage(e.getMessage());
      } finally {
        for (int i = 0; i < mInStreams.length; i++) {
          closeInStream(i);
        }
      }

      // Update local thread end time
      mResult.setEndMs(CommonUtils.getCurrentMs());

      // merge local thread result with full result
      mContext.mergeThreadResult(mResult);
      return null;
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
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

      int i = 0;
      while (!Thread.currentThread().isInterrupted()
          && CommonUtils.getCurrentMs() < mContext.getEndMs() && i < mFilePaths.length) {
        int ioBytes = applyOperation(i);
        long currentMs = CommonUtils.getCurrentMs();
        // Start recording after the warmup
        if (currentMs > recordMs) {
          if (ioBytes > 0) {
            mResult.incrementIOBytes(ioBytes);
          }
        }
        i++;
        if (i >= mFilePaths.length) {
          i = 0;
        }
      }
    }

    /**
     * Read the file by the offset and length based on the given index.
     * @param i the index of the path, offset and length of the target file
     * @return the actual red byte number
     */
    private int applyOperation(int i) throws IOException {
      Path filePath = mFilePaths[i];
      int offset = mOffsets[i];
      int length = mLengths[i];

      if (mInStreams[i] == null) {
        mInStreams[i] = mFs.open(filePath);
      }

      int bytesRead = 0;
      if (mIsRandomReed) {
        while (length > 0) {
          int actualReadLength = mInStreams[i]
              .read(offset, mBuffers[i], 0, mBuffers[i].length);
          if (actualReadLength < 0) {
            closeInStream(i);
            break;
          } else {
            bytesRead += actualReadLength;
            length -= actualReadLength;
            offset += actualReadLength;
          }
        }
      } else {
        while (true) {
          int actualReadLength = mInStreams[i].read(mBuffer);
          if (actualReadLength < 0) {
            closeInStream(i);
            mInStreams[i] = mFs.open(filePath);
            break;
          } else {
            bytesRead += actualReadLength;
          }
        }
      }
      return bytesRead;
    }

    private void closeInStream(int i) {
      try {
        if (mInStreams[i] != null) {
          mInStreams[i].close();
        }
      } catch (IOException e) {
        mResult.addErrorMessage(e.getMessage());
      } finally {
        mInStreams[i] = null;
      }
    }
  }
}
