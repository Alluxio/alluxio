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

package alluxio.fsmaster;

import alluxio.grpc.GetStatusPResponse;

import io.grpc.stub.ServerCallStreamObserver;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link alluxio.master.file.FileSystemMaster}.
 */
@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 6, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
public class FileSystemMasterBench {

  @State(Scope.Thread)
  public static class ThreadState {
    private static final long RAND_SEED = 12345;
    long mNxtFileId;
    int mMyId = 0;
//    int mThreadCount = 0;
    int mFileCount;

    private long getNxtId() {
      mNxtFileId++;
      return mNxtFileId % mFileCount;
    }

    @Setup(Level.Trial)
    public void setup(FileSystem fs, ThreadParams params) {
      mMyId = params.getThreadIndex();
      mNxtFileId = new Random(RAND_SEED + mMyId).nextInt(fs.mFileCount);
//      mThreadCount = params.getThreadCount();
      mFileCount = fs.mFileCount;
    }
  }

  @Benchmark
  public void getStatusBench(FileSystem fs, ThreadState ts, Blackhole bh) {
    ServerCallStreamObserver<GetStatusPResponse> so = createStreamObserver(bh);
    fs.mBase.getStatus(ts.getNxtId(), so);
  }

  @State(Scope.Benchmark)
  public static class FileSystem {
    @Param({"100", "10000", "1000000"})
    public int mFileCount;

    public FileSystemMasterBase mBase;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      mBase = new FileSystemMasterBase();
      for (int i = 0; i < mFileCount; i++) {
        mBase.createFile(i);
      }
    }

    @TearDown
    public void tearDown() throws Exception {
      mBase.tearDown();
    }
  }

  private <T> ServerCallStreamObserver<T> createStreamObserver(Blackhole bh) {
    return new ServerCallStreamObserver<T>() {
      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public void setOnCancelHandler(Runnable onCancelHandler) {
        bh.consume(onCancelHandler);
      }

      @Override
      public void setCompression(String compression) {
        bh.consume(compression);
      }

      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public void setOnReadyHandler(Runnable onReadyHandler) {
        bh.consume(onReadyHandler);
      }

      @Override
      public void disableAutoInboundFlowControl() {
        bh.consume(new Object());
      }

      @Override
      public void request(int count) {
        bh.consume(count);
      }

      @Override
      public void setMessageCompression(boolean enable) {
        bh.consume(enable);
      }

      @Override
      public void onNext(T value) {
        bh.consume(value);
      }

      @Override
      public void onError(Throwable t) {
        bh.consume(t);
      }

      @Override
      public void onCompleted() {
        bh.consume(new Object());
      }
    };
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
        .parent(argsCli)
        .include(FileSystemMasterBench.class.getName())
        .result("results.json")
        .resultFormat(ResultFormatType.JSON)
        .build();
    new Runner(opts).run();
  }
}
