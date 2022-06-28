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

import com.google.common.base.Preconditions;
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
 * This benchmark measures the performance of RocksDB single key
 * read and write operations that are interleaved.
 * The keys are InodeIDs and the values are the Inode themselves.
 * Each thread creates mFileCount key/value pairs, then performs
 * operations over these, adds and deletes will be performed at
 * approximately the same rate to keep the total number of files
 * steady.
 * The following options can be varied.
 * mWritePercentage - the percentage of write operations, half of these will
 *  be adds and half will be deletes, the remaining will be reads.
 * mWriteSerialization - enable or disable ProtoBuf serialization of writes.
 * mUserSerialization - enable or disable ProtoBuf serialization of reads.
 * mFileCount - the initial number of inodes stored in RocksDB for each
 *  thread.
 * mIsDirectory - have the Inodes represent directories or files.
 * mUseZipf - if to use a Zipfian distribution when choosing the keys to read.
 *  The more likely keys to be read will be the ones written last,
 *  meaning that they will more likely be in the RocksDB memtable.
 * mRocksConfig - see {@link RocksBenchConfig}
 */
public class RocksBenchReadWrite {
  @State(Scope.Thread)
  public static class ThreadState {

    private static final long RAND_SEED = 12345;
    Random mRandom;
    int mMyId;
    int mThreadCount;
    int mFileCount;
    byte[] mInodeRead;
    long mNxtFileId;
    long mMinFileId;
    MutableInode<?> mMyInode;
    byte[] mMyInodeBytes;

    private long getNxtId(Db db) {
      long nxtId;
      if (db.mUseZipf) {
        nxtId = (db.mDist.nextValue()) % (mNxtFileId - mMinFileId);
      } else {
        nxtId = mRandom.nextInt((int) (mNxtFileId - mMinFileId));
      }
      return mMinFileId + nxtId;
    }

    private void performOp(Db db, Blackhole bh) {
      int opType = mRandom.nextInt(100);
      if (db.mWritePercentage > opType) {
        // see if add or delete
        if ((mNxtFileId - mMinFileId) == 1 || mRandom.nextInt(100) < 50) {
          // add
          mNxtFileId++;
          if (db.mWriteSerialization) {
            db.mBase.writeInode(mNxtFileId, mThreadCount, mMyId, mMyInode);
          } else {
            db.mBase.writeBytes(mNxtFileId, mThreadCount, mMyId, mMyInodeBytes);
          }
        } else {
          // delete
          db.mBase.remove(mMinFileId, mThreadCount, mMyId);
          mMinFileId++;
        }
      } else {
        switch (db.mReadType) {
          case SER_READ:
            bh.consume(db.mBase.readInode(mNxtFileId, getNxtId(db), mMinFileId,
                mThreadCount, mMyId));
            break;
          case NO_SER_READ:
            bh.consume(db.mBase.readInodeBytes(mNxtFileId, getNxtId(db), mMinFileId,
                mThreadCount, mMyId));
            break;
          case SER_NO_ALLOC_READ:
            bh.consume(db.mBase.readInode(mNxtFileId, getNxtId(db), mMinFileId, mThreadCount,
                mMyId, mInodeRead));
            break;
          case NO_SER_NO_ALLOC_READ:
            bh.consume(db.mBase.readInodeBytes(mNxtFileId, getNxtId(db), mMinFileId, mThreadCount,
                mMyId, mInodeRead));
            break;
          default:
            throw new RuntimeException("Unknown mReadType");
        }
      }
    }

    @Setup(Level.Trial)
    public void setup(Db db, ThreadParams params) {
      mMyId = params.getThreadIndex();
      mRandom = new Random(RAND_SEED + mMyId);
      mFileCount = db.mFileCount;
      mInodeRead = new byte[1024];
      mThreadCount = params.getThreadCount();
      mMyInode = genInode(db.mIsDirectory);
      mMyInodeBytes = mMyInode.toProto().toByteArray();
      mMinFileId = 0;
      mNxtFileId = mFileCount - 1;

      for (long i = 0; i < mFileCount; i++) {
        db.mBase.writeBytes(i, mThreadCount, mMyId, mMyInodeBytes);
      }
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

    @Param({"false", "true"})
    public boolean mWriteSerialization;

    @Param({"100", "100000", "1000000"})
    public int mFileCount;

    @Param({"true", "false"})
    public boolean mIsDirectory;

    @Param({"1", "10"})
    public int mWritePercentage;

    @Param({RocksBenchConfig.JAVA_CONFIG, RocksBenchConfig.BASE_CONFIG,
        RocksBenchConfig.EMPTY_CONFIG, RocksBenchConfig.BLOOM_CONFIG})
    public String mRocksConfig;

    RocksBenchBase mBase;

    ZipfianGenerator mDist;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      Preconditions.checkState(mWritePercentage >= 0 && mWritePercentage <= 100,
          "write percentage must be between 0 and 100");
      if (mUseZipf) {
        mDist = new ZipfianGenerator(mFileCount);
      }
      mBase = new RocksBenchBase(mRocksConfig);
    }

    @TearDown(Level.Trial)
    public void after() {
      mBase.after();
      mBase = null;
    }
  }

  @Benchmark
  public void testMethod(Db db, ThreadState ts, Blackhole bh) {
    ts.performOp(db, bh);
  }

  public static void main(String []args) throws RunnerException {
    Options opt = new OptionsBuilder().include(RocksBenchReadWrite.class.getSimpleName())
        .forks(1).addProfiler(StackProfiler.class).build();
    new Runner(opt).run();
  }
}
