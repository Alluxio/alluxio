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

package alluxio.invalidation.cache;

import alluxio.AlluxioURI;
import alluxio.BaseFileStructure;
import alluxio.BaseThreadState;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.options.DescendantType;
import alluxio.master.file.meta.InvalidationSyncCache;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class InvalidationSyncCacheBench {

  @State(Scope.Thread)
  public static class ThreadState extends BaseThreadState {

    int mDirectorySyncCount;
    int mFileSyncCount;
    int mTotalSyncChecks;
    Random mRand = new Random();

    @Setup(Level.Trial)
    public void trialSetup() {
      mRand = new Random();
    }

    @Setup(Level.Iteration)
    public void setup() {
      mDirectorySyncCount = 0;
      mFileSyncCount = 0;
      mTotalSyncChecks = 0;
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
      System.out.printf("Directory sync needed %d, file sync needed %d,"
              + " total sync checks %d, %f percent syncs%n",
          mDirectorySyncCount, mFileSyncCount, mTotalSyncChecks,
          100 * (((float) mFileSyncCount + mDirectorySyncCount) / (float) mTotalSyncChecks));
    }

    boolean nextOpIsCheckSync(FileStructure fs) {
      return mRand.nextInt(100) < fs.mCheckSync;
    }

    boolean nextOpIsDirectory(FileStructure fs) {
      return mRand.nextInt(100) < fs.mDirSync;
    }

    AlluxioURI nextPath(BaseFileStructure fs, boolean isDirectory) {
      int depth = nextDepth(fs);
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < depth; i++) {
        builder.append("/nxt").append(nextWidth(fs));
      }
      if (isDirectory) {
        builder.append("/");
      } else {
        builder.append("/file").append(nextFileId(fs, depth));
      }
      return new AlluxioURI(builder.toString());
    }
  }

  @State(Scope.Benchmark)
  public static class FileStructure extends BaseFileStructure {
    InvalidationSyncCache mCache;

    @Param({"100", "1000"})
    public int mCacheSize;

    @Param({"70", "80"})
    public int mCheckSync;

    @Param({"5"})
    public int mDirSync;

    @Param({"UNIFORM", "ZIPF"})
    public Distribution mInvalDist;

    @Param({"1000"})
    public int mInvalCount;

    BaseFileStructure mInvalidationStructure;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
      Configuration.set(PropertyKey.MASTER_UFS_PATH_CACHE_CAPACITY, mCacheSize);

      mInvalidationStructure = new BaseFileStructure();
      mInvalidationStructure.mDistribution = mInvalDist;
      mInvalidationStructure.mFileCount = mInvalCount;
      mInvalidationStructure.mDepth = mDepth;
      mInvalidationStructure.init();
      mCache = new InvalidationSyncCache(new AtomicClock());
      mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
          mCache.recordStartSync(), null, false);

      // first approximately fill the cache
      BaseFileStructure fs = new BaseFileStructure();
      fs.mDistribution = Distribution.UNIFORM;
      fs.mFileCount = mInvalCount;
      fs.mDepth = mDepth;
      fs.mWidth = mWidth;
      fs.init();
      ThreadState ts = new ThreadState();
      int fillSize = Math.min(mCacheSize, 5000000);
      System.out.println("Filling cache with " + fillSize + " elements");
      for (int i = 0; i < fillSize; i++) {
        AlluxioURI nextPath = ts.nextPath(fs, false);
        mCache.notifyInvalidation(nextPath);
      }
      System.out.println("Done filling cache");
    }

    static class AtomicClock extends Clock {

      private final AtomicLong mTime = new AtomicLong();

      @Override
      public long millis() {
        return mTime.incrementAndGet();
      }

      @Override
      public ZoneId getZone() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Instant instant() {
        throw new UnsupportedOperationException();
      }
    }
  }

  @Benchmark
  public void fileInvalidationBench(FileStructure fs, ThreadState ts) throws Exception {
    if (ts.nextOpIsCheckSync(fs)) {
      boolean isDirectory = ts.nextOpIsDirectory(fs);
      AlluxioURI path = ts.nextPath(fs, isDirectory);
      if (fs.mCache.shouldSyncPath(path, Long.MAX_VALUE, DescendantType.ONE).isShouldSync()) {
        if (isDirectory) {
          ts.mDirectorySyncCount++;
        } else {
          ts.mFileSyncCount++;
        }
        fs.mCache.notifySyncedPath(path, DescendantType.ONE, fs.mCache.recordStartSync(), null,
            !isDirectory);
      }
      ts.mTotalSyncChecks++;
    } else {
      AlluxioURI path = ts.nextPath(fs.mInvalidationStructure, false);
      fs.mCache.notifyInvalidation(path);
    }
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
        .parent(argsCli)
        .include(InvalidationSyncCacheBench.class.getName())
        .result("results.json")
        .resultFormat(ResultFormatType.JSON)
        .build();
    new Runner(opts).run();
  }
}
