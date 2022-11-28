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

package alluxio.heapinodestore;

import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.metastore.ReadOption;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.master.metastore.heap.HeapInodeStoreEclipseHashMap;

import org.apache.lucene.util.RamUsageEstimator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Fork(value = 3)
@Warmup(iterations = 0)
@Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
public class HeapInodeStoreBench {
  @State(Scope.Benchmark)
  public static class HeapInodeStoreRemoveState {
    @Param("1000000")
    public long mTest;

    public HeapInodeStore mHeapInodeStore;

    @Setup
    public void before() {
      mHeapInodeStore = new HeapInodeStore();
      for (long i = mTest; i > 0; i--) {
        mHeapInodeStore.writeInode(new MutableInodeFile(i));
      }
    }

    @TearDown
    public void after() {
      System.out.println("");
      System.out.println("ShallowSize: " + RamUsageEstimator.shallowSizeOf(mHeapInodeStore));
      System.out.println("Object size: " + RamUsageEstimator.sizeOfObject(mHeapInodeStore));
      System.out.print("Throughput: ");
      mHeapInodeStore = null;
    }
  }

  @State(Scope.Benchmark)
  public static class HeapInodeStoreWriteState {
    @Param("1000000")
    public long mTest;
    public long mCounter = 0;
    public HeapInodeStore mHeapInodeStore;

    @Setup
    public void before() {
      mHeapInodeStore = new HeapInodeStore();
    }

    @TearDown
    public void after() {
      System.out.println("");
      System.out.println("ShallowSize: " + RamUsageEstimator.shallowSizeOf(mHeapInodeStore));
      System.out.println("Object size: " + RamUsageEstimator.sizeOfObject(mHeapInodeStore));
      System.out.print("Throughput: ");
      mHeapInodeStore = null;
    }
  }

  @State(Scope.Benchmark)
  public static class HeapInodeStoreGetState {
    @Param("1000000")
    public long mTest;
    public HeapInodeStore mHeapInodeStore;
    public ReadOption mOption;

    @Setup
    public void before() {
      mHeapInodeStore = new HeapInodeStore();
      mOption = ReadOption.newBuilder().build();
      for (long i = mTest; i > 0; i--) {
        mHeapInodeStore.writeInode(new MutableInodeFile(i));
      }
    }

    @TearDown
    public void after() {
      System.out.println("");
      System.out.println("ShallowSize: " + RamUsageEstimator.shallowSizeOf(mHeapInodeStore));
      System.out.println("Object size: " + RamUsageEstimator.sizeOfObject(mHeapInodeStore));
      System.out.print("Throughput: ");
      mHeapInodeStore = null;
    }
  }

  @State(Scope.Benchmark)
  public static class HeapInodeStoreAllState {
    @Param("1000000")
    public long mTest;

    public HeapInodeStore mHeapInodeStore;

    @Setup
    public void before() {
      mHeapInodeStore = new HeapInodeStore();
      for (long i = mTest; i > 0; i--) {
        mHeapInodeStore.writeInode(new MutableInodeFile(i));
      }
    }

    @TearDown
    public void after() {
      System.out.println("");
      System.out.println("ShallowSize: " + RamUsageEstimator.shallowSizeOf(mHeapInodeStore));
      System.out.println("Object size: " + RamUsageEstimator.sizeOfObject(mHeapInodeStore));
      System.out.print("Throughput: ");
      mHeapInodeStore = null;
    }
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long HeapInodeStroreRemoveBench(HeapInodeStoreRemoveState hb) {
    long i = hb.mTest;
    for (; i > 0; i--) {
      hb.mHeapInodeStore.remove(i);
    }
    return i;
  }

  @Benchmark
  public long HeapInodeStoreWriteBench(HeapInodeStoreWriteState hb) {
    hb.mHeapInodeStore.writeInode(new MutableInodeFile(hb.mCounter));
    hb.mCounter += 1;
    return hb.mCounter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long HeapInodeStoreGetBench(HeapInodeStoreGetState hb) {
    long i = hb.mTest;
    for (; i > 0; i--) {
      hb.mHeapInodeStore.getMutable(i, hb.mOption);
    }
    return i;
  }

  @Benchmark
  public void HeapInodeStoreAllBench(HeapInodeStoreAllState hb) {
    Set tmp = hb.mHeapInodeStore.allInodes();
  }

  @State(Scope.Benchmark)
  public static class HeapInodeStoreEclipseRemoveState {
    @Param("1000000")
    public long mTest;

    public HeapInodeStoreEclipseHashMap mHeapInodeStore;

    @Setup
    public void before() {
      mHeapInodeStore = new HeapInodeStoreEclipseHashMap();
      for (long i = mTest; i > 0; i--) {
        mHeapInodeStore.writeInode(new MutableInodeFile(i));
      }
    }

    @TearDown
    public void after() {
      System.out.println("");
      System.out.println("ShallowSize: " + RamUsageEstimator.shallowSizeOf(mHeapInodeStore));
      System.out.println("Object size: " + RamUsageEstimator.sizeOfObject(mHeapInodeStore));
      System.out.print("Throughput: ");
      mHeapInodeStore = null;
    }
  }

  @State(Scope.Benchmark)
  public static class HeapInodeStoreEclipseWriteState {
    @Param("1000000")
    public long mTest;
    public long mCounter = 0;
    public HeapInodeStoreEclipseHashMap mHeapInodeStore;

    @Setup
    public void before() {
      mHeapInodeStore = null;
      mHeapInodeStore = new HeapInodeStoreEclipseHashMap();
    }

    @TearDown
    public void after() {
      System.out.println("");
      System.out.println("ShallowSize: " + RamUsageEstimator.shallowSizeOf(mHeapInodeStore));
      System.out.println("Object size: " + RamUsageEstimator.sizeOfObject(mHeapInodeStore));
      System.out.print("Throughput: ");
      mHeapInodeStore = null;
    }
  }

  @State(Scope.Benchmark)
  public static class HeapInodeStoreEclipseGetState {
    @Param("1000000")
    public long mTest;
    public HeapInodeStoreEclipseHashMap mHeapInodeStore;
    public ReadOption mOption;

    @Setup
    public void before() {
      mHeapInodeStore = new HeapInodeStoreEclipseHashMap();
      mOption = ReadOption.newBuilder().build();
      for (long i = mTest; i > 0; i--) {
        mHeapInodeStore.writeInode(new MutableInodeFile(i));
      }
    }

    @TearDown
    public void after() {
      System.out.println("");
      System.out.println("ShallowSize: " + RamUsageEstimator.shallowSizeOf(mHeapInodeStore));
      System.out.println("Object size: " + RamUsageEstimator.sizeOfObject(mHeapInodeStore));
      System.out.print("Throughput: ");
      mHeapInodeStore = null;
    }
  }

  @State(Scope.Benchmark)
  public static class HeapInodeStoreEclipseAllState {
    @Param("1000000")
    public long mTest;

    public HeapInodeStoreEclipseHashMap mHeapInodeStore;

    @Setup
    public void before() {
      mHeapInodeStore = new HeapInodeStoreEclipseHashMap();
      for (long i = mTest; i > 0; i--) {
        mHeapInodeStore.writeInode(new MutableInodeFile(i));
      }
    }

    @TearDown
    public void after() {
      System.out.println("");
      System.out.println("ShallowSize: " + RamUsageEstimator.shallowSizeOf(mHeapInodeStore));
      System.out.println("Object size: " + RamUsageEstimator.sizeOfObject(mHeapInodeStore));
      System.out.print("Throughput: ");
      mHeapInodeStore = null;
    }
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long HeapInodeStroreEclipseRemoveBench(HeapInodeStoreEclipseRemoveState hb) {
    long i = hb.mTest;
    for (; i > 0; i--) {
      hb.mHeapInodeStore.remove(i);
    }
    return i;
  }

  @Benchmark
  public long HeapInodeStoreEclipseWriteBench(HeapInodeStoreEclipseWriteState hb) {
    hb.mHeapInodeStore.writeInode(new MutableInodeFile(hb.mCounter));
    hb.mCounter += 1;
    return hb.mCounter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long HeapInodeStoreEclipseGetBench(HeapInodeStoreEclipseGetState hb) {
    long i = hb.mTest;
    for (; i > 0; i--) {
      hb.mHeapInodeStore.getMutable(i, hb.mOption);
    }
    return i;
  }

  @Benchmark
  public void HeapInodeStoreEclipseAllBench(HeapInodeStoreEclipseAllState hb) {
    Set tmp = hb.mHeapInodeStore.allInodes();
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
            .parent(argsCli)
            .include(HeapInodeStoreBench.class.getName())
            .result("results.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
    new Runner(opts).run();
  }
}
