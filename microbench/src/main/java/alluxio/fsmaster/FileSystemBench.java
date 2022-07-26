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

import alluxio.AlluxioURI;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.Configuration;
import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.concurrent.Semaphore;

@SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
public class FileSystemBench {
  @State(Scope.Benchmark)
  public static class FileSystem {
    /**
     * Use standalone parameters if you've started a standalone server using the main function in
     * {@link FileSystemBase}.
     */
    @Param({""})
    String mStandaloneServerHost;
    @Param({"0"})
    int mStandaloneServerPort;

    @Param({ "ALLUXIO_GRPC_SERVER", "BASIC_GRPC_SERVER" })
    public FileSystemBase.ServerType mServerType;
    @Param({"2"})
    public int mNumGrpcChannels;
    @Param({"100"})
    public int mNumConcurrentCalls;

    FileSystemBase mBase = new FileSystemBase();

    @Setup(Level.Trial)
    public void setup() throws Exception {
      Assert.assertTrue("if standalone server address is specified, host must be specified and "
              + "the port must be greater than 0",
          mServerType != FileSystemBase.ServerType.STANDALONE
              || (!mStandaloneServerHost.isEmpty() && mStandaloneServerPort > 0));
      mBase.init(mServerType, mNumGrpcChannels, mStandaloneServerHost, mStandaloneServerPort);
    }

    @TearDown
    public void tearDown() {
      mBase.tearDown();
    }
  }

  @State(Scope.Thread)
  public static class ThreadState {
    public final AlluxioURI mURI = new AlluxioURI("/");
    public final GetStatusPOptions mOpts = GetStatusPOptions.getDefaultInstance();
    Semaphore mSemaphore;

    @Setup(Level.Trial)
    public void setup(FileSystem fs) {
      mSemaphore = new Semaphore(fs.mNumConcurrentCalls);
    }
  }

  @State(Scope.Thread)
  public static class AlluxioBenchThreadState extends ThreadState {
    alluxio.client.file.FileSystem mFs;

    @Setup(Level.Trial)
    public void setup() {
      mFs = alluxio.client.file.FileSystem.Factory.create(Configuration.global());
    }

    @TearDown
    public void tearDown() throws IOException {
      mFs.close();
    }
  }

  @Benchmark
  public void alluxioGetStatusBench(Blackhole bh, AlluxioBenchThreadState ts) throws Exception {
    ts.mSemaphore.acquire();
    bh.consume(ts.mFs.getStatus(ts.mURI, ts.mOpts));
    ts.mSemaphore.release();
  }

  @State(Scope.Thread)
  public static class BlockingBenchThreadState extends ThreadState {
    FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceBlockingStub mBlockingStub;

    @Setup(Level.Trial)
    public void setup(FileSystem fs, ThreadParams params) {
      int index = params.getThreadIndex() % fs.mNumGrpcChannels;
      mBlockingStub =
          FileSystemMasterClientServiceGrpc.newBlockingStub(fs.mBase.mChannels.get(index));
    }
  }

  @Benchmark
  public void blockingGetStatusBench(Blackhole bh, BlockingBenchThreadState ts) throws Exception {
    ts.mSemaphore.acquire();
    bh.consume(ts.mBlockingStub.getStatus(GetStatusPRequest.newBuilder().setPath(ts.mURI.getPath())
        .setOptions(ts.mOpts).build()));
    ts.mSemaphore.release();
  }

  @State(Scope.Thread)
  public static class AsyncBenchThreadState extends ThreadState {
    FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceFutureStub mAsyncStub;

    @Setup(Level.Trial)
    public void setup(FileSystem fs, ThreadParams params) {
      int index = params.getThreadIndex() % fs.mNumGrpcChannels;
      mAsyncStub =
          FileSystemMasterClientServiceGrpc.newFutureStub(fs.mBase.mChannels.get(index));
    }
  }

  @Benchmark
  public void asyncGetStatusBench(Blackhole bh, AsyncBenchThreadState ts) throws Exception {
    ts.mSemaphore.acquire();
    ListenableFuture<GetStatusPResponse> status =
        ts.mAsyncStub.getStatus(GetStatusPRequest.newBuilder().setPath(ts.mURI.getPath())
            .setOptions(ts.mOpts).build());
    Futures.addCallback(
        status,
        new FutureCallback<GetStatusPResponse>() {
          @Override
          public void onSuccess(GetStatusPResponse result) {
            bh.consume(result);
            ts.mSemaphore.release();
          }

          @Override
          public void onFailure(@NotNull Throwable t) {
            ts.mSemaphore.release();
            throw new RuntimeException(t);
          }
        }, MoreExecutors.directExecutor());
  }

  public static void main(String[] args) throws Exception {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
        .parent(argsCli)
        .include(FileSystemBench.class.getName())
        .result("results.json")
        .resultFormat(ResultFormatType.JSON)
        .build();
    new Runner(opts).run();
  }
}
