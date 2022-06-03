package alluxio.inode;

import static alluxio.inode.InodeBenchBase.HEAP;
import static alluxio.inode.InodeBenchBase.ROCKS;
import static alluxio.inode.InodeBenchBase.ROCKSCACHE;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import site.ycsb.generator.ZipfianGenerator;

import java.util.Random;

/**
 * This benchmark measures the time it takes to read inodes from
 * a tree like structure, this includes locking and traversing the
 * read path.
 * The following parameters can be varied:
 * mType - the type of inode storage to use
 * mSingleFile - if true the operation measured is to traverse the tree and return
 *   a single inode. If false the operation measured is to traverse the tree and list
 *   the files in the reached directory.
 * mDepth - the number of levels in the inode tree
 * mFileCount - the number of inodes created at each depth
 * mUseZipf - if true depths and inodes to be read will be chosen
 *   according to a Zipfian distribution.
 */
public class InodeBenchRead {

  private static final long RAND_SEED = 12345;

  @State(Scope.Thread)
  public static class ThreadState {
    long[] mNxtFileId;
    int mMyId;
    int mNxtDepth;
    int mNumFiles;

    @Setup(Level.Iteration)
    public void setup(Db db, ThreadParams params) {
      mNumFiles = db.mFileCount;
      mNxtFileId = new long[db.mDepth + 1];
      mMyId = params.getThreadIndex();
      Random rand = new Random(RAND_SEED + mMyId);
      for (int i = 0; i <= db.mDepth; i++) {
        mNxtFileId[i] = rand.nextInt(mNumFiles);
      }
      mNxtDepth = rand.nextInt(db.mDepth + 1);
    }

    private int nextDepth(Db db) {
      if (db.mUseZipf) {
        return db.mDistDepth.nextValue().intValue();
      }
      mNxtDepth = (mNxtDepth + 1) % (db.mDepth + 1);
      return mNxtDepth;
    }

    private long nxtFileId(Db db) {
      if (db.mUseZipf) {
        return db.mDist.nextValue();
      }
      mNxtFileId[mNxtDepth] = (mNxtFileId[mNxtDepth] + 1) % db.mFileCount;
      return mNxtFileId[mNxtDepth];
    }

    @TearDown(Level.Iteration)
    public void after() {
    }
  }

  @State(Scope.Benchmark)
  public static class Db {

    @Param({"true", "false"})
    public boolean mSingleFile;

    @Param({HEAP, ROCKS, ROCKSCACHE})
    public String mType;

    @Param({RocksBenchConfig.JAVA_CONFIG, RocksBenchConfig.BASE_CONFIG,
        RocksBenchConfig.EMPTY_CONFIG, RocksBenchConfig.BLOOM_CONFIG})
    public String mRocksConfig;

    @Param({"0", "1", "10"})
    public int mDepth;

    @Param({"10", "100", "1000"})
    public int mFileCount;

    @Param({"false", "true"})
    public boolean mUseZipf;

    ZipfianGenerator mDist;
    ZipfianGenerator mDistDepth;
    InodeBenchBase mBase;

    @Setup(Level.Iteration)
    public void setup() throws Exception {
      mBase = new InodeBenchBase(mType, mRocksConfig);
      mBase.createBasePath(mDepth);
      if (mUseZipf) {
        mDist = new ZipfianGenerator(mFileCount);
        mDistDepth = new ZipfianGenerator(mDepth);
      }
      for (int d = 0; d <= mDepth; d++) {
        for (long i = 0; i < mFileCount; i++) {
          mBase.writeFile(0, d, i);
        }
      }
    }

    @TearDown(Level.Iteration)
    public void after() throws Exception {
      mBase.after();
      mBase = null;
    }
  }

  @Benchmark
  public void testMethod(Db db, ThreadState ts, Blackhole bh) throws Exception {
    if (db.mSingleFile) {
      bh.consume(db.mBase.getFile(0, ts.nextDepth(db), ts.nxtFileId(db)));
    } else {
      db.mBase.listDir(ts.nextDepth(db), bh::consume);
    }
    ts.mNxtFileId[ts.mNxtDepth] = (ts.mNxtFileId[ts.mNxtDepth] + 1) % db.mFileCount;
    ts.mNxtDepth = (ts.mNxtDepth + 1) % (db.mDepth + 1);
  }

  public static void main(String []args) throws RunnerException {
    Options opt = new OptionsBuilder().include(InodeBenchRead.class.getSimpleName())
        .forks(1).build();
    new Runner(opt).run();
  }
}
