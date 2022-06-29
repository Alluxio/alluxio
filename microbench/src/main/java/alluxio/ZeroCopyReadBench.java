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

package alluxio;

import alluxio.grpc.Chunk;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Drainable;
import io.grpc.MethodDescriptor;
import io.netty.buffer.Unpooled;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

/**
 * Benchmarks for {@link ReadResponseMarshaller}.
 * <p>
 * This benchmark measures the performance of marshalling & unmarshalling {@link ReadResponse}
 * with varying sizes of chunk data and compares against a default implementation
 * which does not implement zero-copy.
 */
public class ZeroCopyReadBench {
  @State(Scope.Benchmark)
  public static class BenchParams {
    @Param({ "true", "false" })
    public boolean mUseZeroCopy;

    // marshal a read response with big chunk size
    // 1MB, 10MB, 20MB, 50MB, 100MB
    @Param({ "1", "10", "20", "50", "100" })
    public int mChunkSizeMB;

    // random byte generator
    private final Random mRandom = new Random();

    private final OutputStream mOutStream = new NoopOutputStream();

    public int mChunkSizeByte;

    public MethodDescriptor.Marshaller<ReadResponse> mMarshaller;

    public ReadResponse mReadResponse;

    @Setup(Level.Invocation)
    public void setup() {
      mChunkSizeByte = mChunkSizeMB * 1024 * 1024;

      // set up chunk of data
      byte[] bytes = new byte[mChunkSizeByte];
      mRandom.nextBytes(bytes);
      DataBuffer buffer = new NettyDataBuffer(Unpooled.wrappedBuffer(bytes));

      // set up response object
      mReadResponse = ReadResponse
              .newBuilder()
              .setChunk(Chunk.newBuilder().setData(UnsafeByteOperations.unsafeWrap(bytes)))
              .build();

      // set up marshaller
      if (mUseZeroCopy) {
        // zero-copy marshaller implementation
        mMarshaller = new ReadResponseMarshaller();
        // prepare the reference to the buffer for the marshaller to use
        ((ReadResponseMarshaller) mMarshaller).offerBuffer(buffer, mReadResponse);
      } else {
        // default marshaller implementation
        mMarshaller = new MethodDescriptor.Marshaller<ReadResponse>() {
          @Override
          public InputStream stream(ReadResponse value) {
            return new ByteArrayInputStream(value.toByteArray());
          }

          @Override
          public ReadResponse parse(InputStream stream) {
            // not implemented yet
            throw new UnsupportedOperationException();
          }
        };
      }
    }

    @Benchmark
    @Fork(value = 1)
    @Warmup(iterations = 2, time = 3)
    @Measurement(iterations = 5, time = 3)
    @BenchmarkMode(Mode.Throughput)
    public void marshal(BenchParams params) throws IOException {
      try (InputStream is = params.mMarshaller.stream(params.mReadResponse)) {
        if (params.mUseZeroCopy) {
          ((Drainable) is).drainTo(params.mOutStream);
        } else {
          byte[] buffer = new byte[4096];
          int bytesRead;
          while ((bytesRead = is.read(buffer)) != -1) {
            params.mOutStream.write(buffer, 0, bytesRead);
          }
        }
      }
    }
  }

  // test output stream that writes to nowhere
  private static class NoopOutputStream extends OutputStream {
    @Override
    public void write(int b) {
    }
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
            .parent(argsCli)
            .include(ZeroCopyReadBench.class.getName())
            .result("results.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
    new Runner(opts).run();
  }
}
