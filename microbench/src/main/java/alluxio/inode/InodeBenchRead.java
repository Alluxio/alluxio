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

import static alluxio.inode.InodeBenchBase.HEAP;
import static alluxio.inode.InodeBenchBase.ROCKS;
import static alluxio.inode.InodeBenchBase.ROCKSCACHE;

import alluxio.BaseFileStructure;
import alluxio.BaseThreadState;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This benchmark measures the time it takes to read inodes from
 * a tree like structure, this includes locking and traversing the
 * read path.
 * The following parameters can be varied:
 * mSingleFile - if true the operation measured is to traverse the tree and return
 *   a single inode. If false the operation measured is to traverse the tree and list
 *   the files in the reached directory.
 * mDepth - the number of levels in the inode tree
 * mFileCount - the number of inodes created at each depth
 * mUseZipf - if true depths and inodes to be read will be chosen
 *   according to a Zipfian distribution. This means that directories
 *   with shallow depth will be more likely to be chosen, and files with
 *   larger ids are more likely to be chosen (i.e. those written later).
 * mType - the type of inode storage to use
 * mRocksConfig - see {@link RocksBenchConfig}
 */
public class InodeBenchRead {

  @State(Scope.Thread)
  public static class ThreadState extends BaseThreadState { }

  @State(Scope.Benchmark)
  public static class Db extends BaseFileStructure {

    @Param({"true", "false"})
    public boolean mSingleFile;

    @Param({HEAP, ROCKS, ROCKSCACHE})
    public String mType;

    @Param({RocksBenchConfig.JAVA_CONFIG, RocksBenchConfig.BASE_CONFIG,
        RocksBenchConfig.EMPTY_CONFIG, RocksBenchConfig.BLOOM_CONFIG})
    public String mRocksConfig;

    InodeBenchBase mBase;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      mBase = new InodeBenchBase(mType, mRocksConfig);
      mBase.createBasePath(mDepth);
      for (int d = 0; d <= mDepth; d++) {
        for (long i = 0; i < mFileCount; i++) {
          mBase.writeFile(0, d, i);
        }
      }
    }

    @TearDown(Level.Trial)
    public void after() throws Exception {
      mBase.after();
      mBase = null;
    }
  }

  @Benchmark
  public void testMethod(Db db, ThreadState ts, Blackhole bh) throws Exception {
    if (db.mSingleFile) {
      int depth = ts.nextDepth(db);
      bh.consume(db.mBase.getFile(depth, ts.nextFileId(db, depth)));
    } else {
      db.mBase.listDir(ts.nextDepth(db), bh::consume);
    }
  }

  public static void main(String []args) throws RunnerException {
    Options opt = new OptionsBuilder().include(InodeBenchRead.class.getSimpleName())
        .forks(1).build();
    new Runner(opt).run();
  }
}
