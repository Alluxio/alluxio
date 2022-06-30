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

import static alluxio.inode.RocksBenchBase.NO_SER_NO_ALLOC_READ;
import static alluxio.inode.RocksBenchBase.NO_SER_READ;
import static alluxio.inode.RocksBenchBase.SER_NO_ALLOC_READ;
import static alluxio.inode.RocksBenchBase.SER_READ;
import static alluxio.inode.RocksBenchBase.genInode;

import alluxio.master.file.meta.MutableInode;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import site.ycsb.generator.ZipfianGenerator;

import java.io.IOException;
import java.util.Random;

/**
 * This benchmark measures the performance of RocksDB single key accesses.
 * The keys are InodeIDs and the values are the Inode themselves.
 * The following options can be varied.
 * mUserSerialization - enable or disable ProtoBuf serialization of values.
 * mFileCount - the initial number of inodes stored in RocksDB.
 * mIsDirectory - have the Inodes represent directories or files.
 * mUseZipf - if to use a Zipfian distribution when choosing the keys to read.
 *  The more likely keys to be read will be the ones written last,
 *  meaning that they will more likely be in the RocksDB memtable.
 * mRocksConfig - see {@link RocksBenchConfig}
 */
public class RocksBenchRead {
  @State(Scope.Thread)
  public static class ThreadState {

    private static final long RAND_SEED = 12345;
    long mNxtFileId;
    int mMyId = 0;
    int mFileCount;
    byte[] mInodeRead;

    private long getNxtId(Db db) {
      mNxtFileId++;
      if (db.mUseZipf) {
        return db.mDist.nextValue();
      }
      return mNxtFileId % mFileCount;
    }

    @Setup(Level.Trial)
    public void setup(Db db, ThreadParams params) {
      mMyId = params.getThreadIndex();
      mNxtFileId = new Random(RAND_SEED + mMyId).nextInt(db.mFileCount);
      mFileCount = db.mFileCount;
      mInodeRead = new byte[1024];
    }

    @TearDown(Level.Iteration)
    public void after() {
    }
  }

  @State(Scope.Benchmark)
  public static class Db {

    @Param({"false", "true"})
    public boolean mUseZipf;

    @Param({SER_READ, NO_SER_READ, SER_NO_ALLOC_READ, NO_SER_NO_ALLOC_READ})
    public String mReadType;

    @Param({"100", "100000", "1000000"})
    public int mFileCount;

    @Param({"true", "false"})
    public boolean mIsDirectory;

    @Param({RocksBenchConfig.JAVA_CONFIG, RocksBenchConfig.BASE_CONFIG,
        RocksBenchConfig.EMPTY_CONFIG, RocksBenchConfig.BLOOM_CONFIG})
    public String mRocksConfig;

    RocksBenchBase mBase;

    ZipfianGenerator mDist;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      if (mUseZipf) {
        mDist = new ZipfianGenerator(mFileCount);
      }
      MutableInode<?> inode = genInode(mIsDirectory);
      mBase = new RocksBenchBase(mRocksConfig);
      for (long i = 0; i < mFileCount; i++) {
        mBase.writeInode(i, 1, 0, inode);
      }
    }

    @TearDown(Level.Trial)
    public void after() {
      mBase.after();
      mBase = null;
    }
  }

  @Benchmark
  public void testMethod(Db db, ThreadState ts, Blackhole bh) {
    switch (db.mReadType) {
      case SER_READ:
        bh.consume(db.mBase.readInode(ts.mFileCount - 1, ts.getNxtId(db), 0, 1, 0));
        break;
      case NO_SER_READ:
        bh.consume(db.mBase.readInodeBytes(ts.mFileCount - 1, ts.getNxtId(db), 0, 1, 0));
        break;
      case SER_NO_ALLOC_READ:
        bh.consume(db.mBase.readInode(ts.mFileCount - 1, ts.getNxtId(db), 0, 1, 0, ts.mInodeRead));
        break;
      case NO_SER_NO_ALLOC_READ:
        bh.consume(db.mBase.readInodeBytes(ts.mFileCount - 1, ts.getNxtId(db), 0, 1, 0,
            ts.mInodeRead));
        break;
      default:
        throw new RuntimeException("Unknown mReadType");
    }
  }

  public static void main(String []args) throws RunnerException {
    Options opt = new OptionsBuilder().include(RocksBenchRead.class.getSimpleName())
        .forks(1).addProfiler(StackProfiler.class).build();
    new Runner(opt).run();
  }
}
