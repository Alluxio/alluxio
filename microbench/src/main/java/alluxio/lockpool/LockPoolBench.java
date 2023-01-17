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

package alluxio.lockpool;

import alluxio.collections.LockPool;
import alluxio.concurrent.LockMode;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.resource.LockResource;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.generator.ZipfianGenerator;

public class LockPoolBench {

  @State(Scope.Benchmark)
  public static class LockPoolState {
    LockPool<Integer> mLockPool;

    @Param({"1000000"})
    public int mLockRange;

    @Param({"UNIFORM"})
    public Distribution mDistribution;

    public NumberGenerator mLockIdGenerator;

    public enum Distribution { UNIFORM, ZIPF }

    @Setup(Level.Trial)
    public void setupTrial() {
      mLockPool = new LockPool<>(Configuration.getInt(PropertyKey.MASTER_LOCK_POOL_INITSIZE),
          Configuration.getInt(PropertyKey.MASTER_LOCK_POOL_CONCURRENCY_LEVEL));
      if (mDistribution == Distribution.ZIPF) {
        mLockIdGenerator = new ZipfianGenerator(0, mLockRange);
      } else {
        mLockIdGenerator = new UniformLongGenerator(0, mLockRange);
      }
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      System.out.printf("\nLock pool size %d\n", mLockPool.size());
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws Exception {
      mLockPool.close();
    }

    @Benchmark
    public void lockPoolBench(LockPoolState state, Blackhole bh) {
      try (LockResource lock = state.mLockPool.get(mLockIdGenerator.nextValue().intValue(),
          LockMode.READ)) {
        bh.consume(lock);
      }
    }

    public static void main(String[] args) throws RunnerException, CommandLineOptionException {
      Options argsCli = new CommandLineOptions(args);
      Options opts = new OptionsBuilder()
          .parent(argsCli)
          .include(LockPoolBench.class.getName())
          .result("results.json")
          .resultFormat(ResultFormatType.JSON)
          .build();
      new Runner(opts).run();
    }
  }
}
