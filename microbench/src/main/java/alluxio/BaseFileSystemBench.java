package alluxio;

import alluxio.client.file.FileSystem;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;

import net.bytebuddy.utility.RandomString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link alluxio.client.file.BaseFileSystem}.
 */
@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 6, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class BaseFileSystemBench {
  @SuppressWarnings("FieldMayBeFinal") // do not make member variables final in JMH
  private FileSystem mFs = FileSystem.Factory.create(ServerConfiguration.global());
  private AlluxioURI mUri;

  @Setup(Level.Iteration)
  public void prep() {
    mUri = new AlluxioURI("/" + RandomString.make());
  }

  @Benchmark
  public void baseline() {
    // intentionally left blank
  }

  @Benchmark
  public void benchmarkCreateFile(Blackhole bh) throws IOException, AlluxioException {
    bh.consume(mFs.createFile(mUri, CreateFilePOptions.newBuilder().build()));
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    FileSystem system = FileSystem.Factory.create(ServerConfiguration.global());
    try {
      system.createFile(new AlluxioURI("/" + RandomString.make()),
          CreateFilePOptions.newBuilder().build());
    } catch (IOException | AlluxioException e) {
      e.printStackTrace();
    }

    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
        .parent(argsCli)
        .include(BaseFileSystemBench.class.getName())
        .result("results.json")
        .resultFormat(ResultFormatType.JSON)
        .build();
    new Runner(opts).run();
  }
}
