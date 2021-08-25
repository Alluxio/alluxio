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

package alluxio.microbench;

import static alluxio.RpcUtils.callAndReturn;

import alluxio.metrics.MetricsSystem;

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
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link alluxio.RpcUtils}.
 */
@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
public class RpcUtilsBench {
  private static final Logger LOG = LoggerFactory.getLogger(RpcUtilsBench.class);
  private static final String METHOD_NAME = "CheckAccess"; // an arbitrary rpc method name

  private static int sCounter = 0;

  @Benchmark
  public long testCallAndReturn(BenchParams params) throws Exception {
    return callAndReturn(LOG,
        () -> delayWithSideEffect(params.mDelay),
        METHOD_NAME,
        true, // irrelevant as no failure should occur here
        "");
  }

  @Benchmark
  public long delayBaseline(BenchParams params) throws Exception {
    return delayWithSideEffect(params.mDelay);
  }

  private static long delayWithSideEffect(final int delay) {
    sCounter++;
    for (int i = 0; i < delay; i++) {
      // create some side effect to prevent the loop being optimized out
      sCounter++;
    }
    // subtract and effectively set it back to 0
    sCounter -= delay;
    return sCounter;
  }

  @State(Scope.Benchmark)
  public static class BenchParams {
    @Param({ "10000", "50000", "100000", "500000", "1000000"})
    public int mDelay;
  }

  public static void main(String[] args) throws RunnerException {
    Options opts = new OptionsBuilder()
        .include(".*.RpcUtilsBench.*")
        .shouldDoGC(true)
        .result("results.json")
        .resultFormat(ResultFormatType.JSON)
        .build();
    new Runner(opts).run();
  }
}
