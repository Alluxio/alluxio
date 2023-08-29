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

import alluxio.uri.UfsUrl;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
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

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link alluxio.uri.UfsUrl}.
 */

@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 6, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)

public class UfsUrlBench {

  @State(Scope.Benchmark)
  public static class BenchParams {
    @Param({"abc://localhost:8080/1/2/3/4/5",
        "abc://bucket/1/2////3 4/////56 7/"})
    public String mPaths;
  }

  @Benchmark
  public void testCtorOfUfsUrl(Blackhole bh, BenchParams benchParams) throws Exception {
    bh.consume(UfsUrl.createInstance(benchParams.mPaths));
  }

  @Benchmark
  public void testCtorOfAlluxioURI(Blackhole bh, BenchParams benchParams) throws Exception {
    bh.consume(new AlluxioURI(benchParams.mPaths));
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    Options argsFromCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
        .parent(argsFromCli)
        .include(UfsUrlBench.class.getName())
        .result("result.json")
        .resultFormat(ResultFormatType.JSON)
        .build();
    new Runner(opts).run();
  }
}
