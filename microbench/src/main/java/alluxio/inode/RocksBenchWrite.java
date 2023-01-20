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

package alluxio.inode;

import static alluxio.inode.RocksBenchBase.genInode;

import alluxio.master.file.meta.MutableInode;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This benchmark measures the performance of writing single keys/value
 * pairs of inodeId/inode to RocksDB. Each iteration of the benchmark
 * writes keys with increasing ids to the db.
 * The following options can be varied:
 * mIsDirectory - if the inodes written should represent a file or directory
 * mUseSerialization - if true, the measured time will include serializing an inode to bytes.
 * mRocksConfig - see {@link RocksBenchConfig}
 **/
public class RocksBenchWrite {
  @State(Scope.Thread)
  public static class ThreadState {
    long mNxtFileId = 0;
    int mMyId = 0;
    int mThreadCount = 0;
    MutableInode<?> mMyInode;
    byte[] mMyInodeBytes;

    @Param({"true", "false"})
    public boolean mIsDirectory;

    @Setup(Level.Iteration)
    public void setup(ThreadParams params) {
      mNxtFileId = 0;
      mMyId = params.getThreadIndex();
      mThreadCount = params.getThreadCount();
      mMyInode = genInode(mIsDirectory);
      mMyInodeBytes = mMyInode.toProto().toByteArray();
    }

    @TearDown(Level.Iteration)
    public void after() {
      System.out.printf("Insert count for id %d: %d%n", mMyId, mNxtFileId);
    }
  }

  @State(Scope.Benchmark)
  public static class Db {

    @Param({"true"})
    public boolean mUseSerialization;

    @Param({RocksBenchConfig.JAVA_CONFIG})
    public String mRocksConfig;

    RocksBenchBase mBase;

    @Setup(Level.Iteration)
    public void setup() throws Exception {
      mBase = new RocksBenchBase(mRocksConfig);
    }

    @TearDown(Level.Iteration)
    public void after() {
      mBase.after();
      mBase = null;
    }
  }

  @Benchmark
  public void testMethod(Db db, ThreadState ts) {
    if (db.mUseSerialization) {
      db.mBase.writeInode(ts.mNxtFileId, ts.mThreadCount, ts.mMyId, ts.mMyInode);
    } else {
      db.mBase.writeBytes(ts.mNxtFileId, ts.mThreadCount, ts.mMyId, ts.mMyInodeBytes);
    }
    ts.mNxtFileId++;
  }

  public static void main(String []args) throws RunnerException {
    Options opt = new OptionsBuilder().include(RocksBenchWrite.class.getSimpleName())
        .forks(1).addProfiler(StackProfiler.class).build();
    new Runner(opt).run();
  }
}
