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

import alluxio.BaseFileStructure;
import alluxio.BaseThreadState;
import alluxio.master.file.meta.MutableInode;

import com.google.common.base.Preconditions;
import org.junit.Assert;
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
  public static class ThreadState extends BaseThreadState {
    private static final long RAND_SEED = 12345;
    Random mRandom = null;
    int mThreadCount;
    byte[] mInodeRead;
    long mNxtFileId;
    long mMinFileId;
    MutableInode<?> mMyInode;
    byte[] mMyInodeBytes;

    private long getNxtId(Db db) {
      long nxtId = nextFileId(db, 0) % (mNxtFileId - mMinFileId + 1);
      return mMinFileId + nxtId;
    }

    private void performOp(Db db, Blackhole bh) {
      int opType = mRandom.nextInt(100);
      if (db.mWritePercentage > opType) {
        // see if add or delete
        if ((mNxtFileId - mMinFileId) < 1 || mRandom.nextInt(100) < 50) {
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
      mRandom = new Random(RAND_SEED + mMyId);
      mInodeRead = new byte[1024];
      mThreadCount = params.getThreadCount();
      mMyInode = genInode(db.mIsDirectory);
      mMyInodeBytes = mMyInode.toProto().toByteArray();
      mMinFileId = 0;
      mNxtFileId = db.mFileCount - 1;

      for (long i = 0; i < db.mFileCount; i++) {
        db.mBase.writeBytes(i, mThreadCount, mMyId, mMyInodeBytes);
      }
    }

    @TearDown(Level.Iteration)
    public void after() {
    }
  }

  @State(Scope.Benchmark)
  public static class Db extends BaseFileStructure {
    @Param({"0"})
    public int mWidth;

    @Param({"1000"})
    public int mFileCount;

    // is used in read benchmark to simulate different file access patterns
    @Param({"ZIPF"})
    public Distribution mDistribution;
    @Param({SER_READ})
    public String mReadType;

    @Param({"true"})
    public boolean mWriteSerialization;

    @Param({"true", "false"})
    public boolean mIsDirectory;

    @Param({"20"})
    public int mWritePercentage;

    @Param({RocksBenchConfig.JAVA_CONFIG})
    public String mRocksConfig;

    RocksBenchBase mBase;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      Preconditions.checkState(mWritePercentage >= 0 && mWritePercentage <= 100,
          "write percentage must be between 0 and 100");
      Assert.assertTrue("mFileCount has to be greater than 0", 0 < mFileCount);
      super.init(0, mWidth, mFileCount, mDistribution);

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
