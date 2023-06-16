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

package alluxio.dora.meta;

import static alluxio.dora.meta.DoraMetaBenchBase.ROCKS;
import static alluxio.dora.meta.DoraMetaBenchBase.ROCKS_1GB_CACHE;
import static alluxio.dora.meta.DoraMetaBenchBase.UFS_PATH_PREFIX;
import static alluxio.dora.meta.DoraMetaBenchBase.makeFileStatus;

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
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This benchmark measures the time it takes to read/write metadata from the dora worker metastore
 * The following parameters can be varied:
 * mFileCount - the number of file meta created
 * mUseZipf - if true depths and inodes to be read will be chosen
 *   according to a Zipfian distribution. This means that directories
 *   with shallow depth will be more likely to be chosen, and files with
 *   larger ids are more likely to be chosen (i.e. those written later).
 * mType - the type of inode storage to use
 * mOperation - read or write
 */
public class DoraMetaBench {
  @State(Scope.Thread)
  public static class ThreadState extends BaseThreadState { }

  @State(Scope.Benchmark)
  public static class Db extends BaseFileStructure {
    @Param({"10000000"})
    public int mFileCount;

    @Param({"ZIPF"})
    public Distribution mDistribution;

    @Param({ROCKS, ROCKS_1GB_CACHE})
    public String mType;

    @Param({"READ"})
    public String mOperation;

    DoraMetaBenchBase mBase;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      super.init(0, 0, mFileCount, mDistribution);
      mBase = new DoraMetaBenchBase(mType);
      for (int i = 0; i < mFileCount; ++i) {
        mBase.getDoraMetaStore().putDoraMeta(UFS_PATH_PREFIX + i, makeFileStatus());
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
    if (db.mOperation.equals("READ")) {
      bh.consume(db.mBase.getDoraMetaStore().getDoraMeta(UFS_PATH_PREFIX +  ts.nextFileId(db, 0)));
    } else
    {
      db.mBase.getDoraMetaStore().putDoraMeta(
          UFS_PATH_PREFIX +  ts.nextFileId(db, 0), makeFileStatus());
    }
  }

  public static void main(String []args) throws RunnerException {
    Options opt = new OptionsBuilder().include(DoraMetaBench.class.getSimpleName())
        .addProfiler(AsyncProfiler.class, "output=flamegraph")
        .addProfiler(GCProfiler.class)
        .warmupIterations(5)
        .forks(1).threads(1).build();
    new Runner(opt).run();
  }
}
