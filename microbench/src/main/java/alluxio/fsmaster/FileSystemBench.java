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

import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
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

import java.util.concurrent.Semaphore;

public class FileSystemBench {
  @State(Scope.Benchmark)
  public static class FileSystem {
    FileSystemBase mBase = new FileSystemBase();

    @Setup(Level.Trial)
    public void setup() throws Exception {
      mBase.init();
    }

    @TearDown
    public void tearDown() throws Exception {
      mBase.tearDown();
    }
  }

  @State(Scope.Thread)
  public static class ThreadState {
    FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceFutureStub mClient;
    Semaphore mSemaphore;

    @Setup(Level.Trial)
    public void setup(FileSystem fs, ThreadParams params) {
      int index = params.getThreadIndex() % 2;
      mClient = FileSystemMasterClientServiceGrpc.newFutureStub(fs.mBase.mChannels.get(index));
      mSemaphore = new Semaphore(100);
    }
  }

  @Benchmark
  public void getStatusBench(FileSystem fs, Blackhole bh, ThreadState ts) throws Exception {
    ts.mSemaphore.acquire();
    ListenableFuture<GetStatusPResponse> status =
        ts.mClient.getStatus(GetStatusPRequest.newBuilder().setPath(fs.mBase.mURI.getPath())
            .setOptions(GetStatusPOptions.getDefaultInstance()).build());
    Futures.addCallback(
        status,
        new FutureCallback<GetStatusPResponse>() {
          @Override
          public void onSuccess(GetStatusPResponse result) {
            bh.consume(result);
            ts.mSemaphore.release();
          }

          @Override
          public void onFailure(Throwable t) {
            ts.mSemaphore.release();
            throw new RuntimeException(t);
          }
        }, MoreExecutors.directExecutor());
//    bh.consume(fs.mBase.getStatus());
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
