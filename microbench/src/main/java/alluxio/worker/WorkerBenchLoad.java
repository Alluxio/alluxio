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

package alluxio.worker;

import alluxio.grpc.Block;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;

public class WorkerBenchLoad {
  @State(Scope.Benchmark)
  public static class BenchParams {

    @Param({"1", "10", "100"})
    public int mBatchSize;

    @Param({"1000000", "64000000"})
    public int mBlockSize;

    BlockWorkerBase mBase;
    private List<String> mFiles;
    public Random mRandom = new Random(12345);

    @Setup(Level.Trial)
    public void setup() throws Exception {
      mBase = new BlockWorkerBase();
    }

    @Setup(Level.Invocation)
    public void setupInvoc() throws Exception {
      mFiles = mBase.createFile(mBatchSize, mBlockSize);
    }

    @TearDown(Level.Trial)
    public void after() throws Exception {
      mBase.after();
    }
  }

  @Benchmark
  public void testLoad(BenchParams param, Blackhole bh) {
    ArrayList<Block> blocks = new ArrayList<>();
    int blockId = param.mRandom.nextInt();
    for (int i = 0; i < param.mBatchSize; i++) {
      Block block =
          Block.newBuilder().setBlockId(blockId).setBlockSize(param.mBlockSize).setMountId(0)
              .setOffsetInFile(0).setUfsPath(param.mFiles.get(i)).build();
      blocks.add(block);
    }
    bh.consume(param.mBase.mBlockStore.load(blocks, "test", OptionalLong.empty()));
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder().include(WorkerBenchLoad.class.getSimpleName()).addProfiler(
            StackProfiler.class).forks(1).build();
    new Runner(opt).run();
  }
}
