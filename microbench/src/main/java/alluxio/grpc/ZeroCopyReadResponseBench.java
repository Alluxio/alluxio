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

package alluxio.grpc;

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Drainable;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

/**
 * Benchmarks for {@link ReadResponseMarshaller}.
 * <p>
 * This benchmark measures the performance of marshalling & unmarshalling {@link ReadResponse}
 * with varying sizes of chunk data and compares against a default implementation
 * which does not implement zero-copy.
 */
@Fork(value = 1)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
public class ZeroCopyReadResponseBench {

  // Dumb OutputStream that consumes serialized bytes
  private static final ByteArrayOutputStream SINK = new ByteArrayOutputStream(100 * 1024 * 1024);

  // Custom marshaller
  private static final ReadResponseMarshaller ZERO_COPY_MARSHALLER = new ReadResponseMarshaller();

  // Default marshaller that comes with the protobuf definition
  private static final MethodDescriptor.Marshaller<ReadResponse> DEFAULT_MARSHALLER =
          ProtoUtils.marshaller(ReadResponse.getDefaultInstance());

  // Buffer used to drain InputStream manually
  private static final byte[] BUF = new byte[4096];

  @State(Scope.Benchmark)
  public static class BenchParams {
    // marshal a read response with big chunk size
    // 1MB, 10MB, 20MB, 50MB, 100MB
    @Param({ "1", "10", "20", "50", "100" })
    public int mChunkSizeMB;

    // random byte generator
    private final Random mRandom = new Random();

    public int mChunkSizeByte;

    // chunk data buffer
    public DataBuffer mChunkData;

    // ReadResponse object used for marshalling
    public ReadResponse mReadResponse;

    // Serialized InputStream used for unmarshalling
    public InputStream mReadResponseInputStream;

    @Setup(Level.Invocation)
    public void setup() {
      mChunkSizeByte = mChunkSizeMB * 1024 * 1024;

      // set up chunk of data
      byte[] bytes = new byte[mChunkSizeByte];
      mRandom.nextBytes(bytes);
      mChunkData = new NettyDataBuffer(Unpooled.wrappedBuffer(bytes));

      // set up response object
      mReadResponse = ReadResponse
              .newBuilder()
              .setChunk(Chunk.newBuilder().setData(UnsafeByteOperations.unsafeWrap(bytes)))
              .build();

      // set up serialized stream
      mReadResponseInputStream = new ByteArrayInputStream(mReadResponse.toByteArray());
    }
  }

  @Benchmark
  public void marshalZeroCopy(BenchParams params) throws IOException {
    ZERO_COPY_MARSHALLER.offerBuffer(params.mChunkData, params.mReadResponse);
    try (InputStream is = ZERO_COPY_MARSHALLER.stream(params.mReadResponse)) {
      SINK.reset();
      ((Drainable) is).drainTo(SINK);
    }
  }

  @Benchmark
  public void marshalBaselineDrain(BenchParams params) throws IOException {
    try (InputStream is = DEFAULT_MARSHALLER.stream(params.mReadResponse)) {
      SINK.reset();
      ((Drainable) is).drainTo(SINK);
    }
  }

  @Benchmark
  public void marshalBaselineRead(BenchParams params) throws IOException {
    try (InputStream is = DEFAULT_MARSHALLER.stream(params.mReadResponse)) {
      int byteRead;
      SINK.reset();
      while ((byteRead = is.read(BUF)) != -1) {
        SINK.write(BUF, 0, byteRead);
      }
    }
  }

  @Benchmark
  public void unmarshalZeroCopy(BenchParams params, Blackhole blackhole) {
    ReadResponse unmarshalResult;
    // this is the bare-bone response without the underlying chunk
    unmarshalResult = ZERO_COPY_MARSHALLER.parse(params.mReadResponseInputStream);
    // get and combine the chunk
    DataBuffer buf = ZERO_COPY_MARSHALLER.pollBuffer(unmarshalResult);
    unmarshalResult = ZERO_COPY_MARSHALLER.combineData(new DataMessage<>(unmarshalResult, buf));

    blackhole.consume(unmarshalResult);
  }

  @Benchmark
  public void unmarshalBaseline(BenchParams params, Blackhole blackhole) {
    ReadResponse unmarshalResult = DEFAULT_MARSHALLER.parse(params.mReadResponseInputStream);

    blackhole.consume(unmarshalResult);
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
            .parent(argsCli)
            .include(ZeroCopyReadResponseBench.class.getName())
            .result("results.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
    new Runner(opts).run();
  }
}
