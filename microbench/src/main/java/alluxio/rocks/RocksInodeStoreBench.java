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

package alluxio.rocks;

import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.metastore.ReadOption;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.resource.CloseableIterator;

import org.junit.rules.TemporaryFolder;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(value = 3)
@Warmup(iterations = 2)
public class RocksInodeStoreBench {
  @State(Scope.Benchmark)
  public static class RockState {
    protected RocksInodeStore mRock;

    @Param("10000")
    protected long mInodeCount;

    @Setup
    public void before() {
      TemporaryFolder sFolder = new TemporaryFolder();
      try {
        sFolder.create();
        String sBaseDir = sFolder.newFolder().getAbsolutePath();
        mRock = new RocksInodeStore(sBaseDir);
      } catch (Exception e) {
        System.out.println("error when getting base dir: " + e);
      }
      for (long i = 1; i <= mInodeCount; i++) {
        mRock.addChild(0, "test" + i, i);
        mRock.writeInode(MutableInodeDirectory
                .create(i, 0, "test" + i, CreateDirectoryContext.defaults()));
      }
    }

    @TearDown
    public void after() {
      mRock.close();
    }
  }

  @State(Scope.Benchmark)
  public static class RockAddChildState {
    protected RocksInodeStore mRock;

    @Param("10000")
    protected long mInodeCount;

    @Setup
    public void before() {
      TemporaryFolder sFolder = new TemporaryFolder();
      try {
        sFolder.create();
        String sBaseDir = sFolder.newFolder().getAbsolutePath();
        System.out.println("sBaseDir: " + sBaseDir);
        mRock = new RocksInodeStore(sBaseDir);
      } catch (Exception e) {
        System.out.println("error when getting base dir: " + e);
      }
    }

    @TearDown
    public void after() {
      mRock.close();
    }
  }

  @State(Scope.Benchmark)
  public static class RockGetChildIdsState {
    protected RocksInodeStore mRock;

    @Param("100")
    protected long mTreeNumber;

    @Param("100")
    protected long mTreeWidth;

    @Setup
    public void before() {
      TemporaryFolder sFolder = new TemporaryFolder();
      try {
        sFolder.create();
        String sBaseDir = sFolder.newFolder().getAbsolutePath();
        mRock = new RocksInodeStore(sBaseDir);
      } catch (Exception e) {
        System.out.println("error when getting base dir: " + e);
      }
      long tmp;
      long child = mTreeNumber + 1;
      for (long i = 1; i <= mTreeNumber; i++) {
        mRock.addChild(0, "test" + i, i);
        for (long j = 1; j <= mTreeWidth; j++) {
          tmp = i * 100 + j;
          mRock.addChild(i, "test" + child, child);
          child += 1;
        }
      }
    }

    @TearDown
    public void after() {
      mRock.close();
    }
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(10)
  @Benchmark
  public long RockGetMutableBench(RockState rs, Blackhole bh) {
    long counter = 0;
    for (long i = rs.mInodeCount; i > 0; i--) {
      bh.consume(rs.mRock.getMutable(i));
      counter += 1;
    }
    return counter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(10)
  @Benchmark
  public long RockGetChildIdBench(RockState rs, Blackhole bh) {
    long counter = 0;
    Long tmp = Long.valueOf(0);
    Long p = Long.valueOf(0);
    for (long i = rs.mInodeCount; i > 0; i--) {
      // tmp = rs.mRock.getChildId(p, "test" + i, ReadOption.defaults()).get();
      bh.consume(rs.mRock.getChildId(p, "test" + i, ReadOption.defaults()));
      counter += 1;
    }
    return counter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Measurement(iterations = 3)
  @Threads(10)
  @Benchmark
  public long RockGetChildIdsBench(RockGetChildIdsState rs, Blackhole bh) {
    long counter = 0;
    for (long i = rs.mTreeNumber; i > 0; i--) {
      try (CloseableIterator<Long> iter = rs.mRock.getChildIds(i, ReadOption.defaults())) {
        while (iter.hasNext()) {
          bh.consume(iter.next());
        }
        counter += 1;
      } catch (Exception e) {
        System.out.println("when testing: " + e);
      }
    }
    return counter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(10)
  @Benchmark
  public long RockAddChildIdBench(RockAddChildState rs) {
    long counter = 0;
    long limit = rs.mInodeCount;
    for (long i = 1; i <= limit; i++) {
      rs.mRock.addChild(0, "test" + i, i);
      counter += 1;
    }
    return counter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long RockGetIteratorBench(RockState rs, Blackhole bh) {
    long counter = 0;
    CloseableIterator<InodeView> it = rs.mRock.getCloseableIterator();
    while (it.hasNext()) {
      bh.consume(it.next());
    }
    counter += 1;
    return counter;
  }
}