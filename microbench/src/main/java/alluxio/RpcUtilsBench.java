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

import static alluxio.RpcUtils.callAndReturn;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link alluxio.RpcUtils}.
 */
@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 6, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
public class RpcUtilsBench {
  private static final Logger LOG = LoggerFactory.getLogger(RpcUtilsBench.class);
  private static final String METHOD_NAME = "CheckAccess"; // an arbitrary rpc method name

  @Benchmark
  public long testCallAndReturn(BenchParams params) throws Exception {
    return callAndReturn(LOG,
        () -> {
          Blackhole.consumeCPU(params.mDelay);
          return params.mDelay;
        },
        METHOD_NAME,
        true, // irrelevant as no failure should occur here
        "");
  }

  @Benchmark
  public long delayBaseline(BenchParams params) throws Exception {
    Blackhole.consumeCPU(params.mDelay);
    return params.mDelay;
  }

  @State(Scope.Benchmark)
  public static class BenchParams {
    @Param({ "500", "1000", "2000", "4000", "8000", "16000"})
    public long mDelay;
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
        .parent(argsCli)
        .include(RpcUtilsBench.class.getName())
        .shouldDoGC(true)
        .result("results.json")
        .resultFormat(ResultFormatType.JSON)
        .build();
    new Runner(opts).run();
  }
}
