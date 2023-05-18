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

import static alluxio.inode.InodeBenchBase.ROCKSCACHE;

import alluxio.BaseFileStructure;
import alluxio.BaseThreadState;

import org.junit.Assert;
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
    @Param({"10"})
    public int mDepth;

    @Param({"0"})
    public int mWidth;

    @Param({"1000"})
    public int mFileCount;

    @Param({"ZIPF"})
    public Distribution mDistribution;

    @Param({"true", "false"})
    public boolean mSingleFile;

    @Param({ROCKSCACHE})
    public String mType;

    @Param({RocksBenchConfig.JAVA_CONFIG})
    public String mRocksConfig;

    InodeBenchBase mBase;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      super.init(mDepth, mWidth, mFileCount, mDistribution);
      Assert.assertTrue("mFileCount needs to be > 0 if mSingleFile is true",
          !mSingleFile || mFileCount > 0);

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
    int depth = ts.nextDepth(db);
    if (db.mSingleFile) {
      bh.consume(db.mBase.getFile(depth, ts.nextFileId(db, depth)));
    } else {
      db.mBase.listDir(depth, bh::consume);
    }
  }

  public static void main(String []args) throws RunnerException {
    Options opt = new OptionsBuilder().include(InodeBenchRead.class.getSimpleName())
        .forks(1).build();
    new Runner(opt).run();
  }
}
