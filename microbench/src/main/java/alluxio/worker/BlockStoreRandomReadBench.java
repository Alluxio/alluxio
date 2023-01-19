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

package alluxio.worker;

import alluxio.AlluxioTestDirectory;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.io.BlockReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

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
@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
public class BlockStoreRandomReadBench {
  /** Seed for random number generator. */
  private static final long SEED = 30;

  /** Consumer of block data. */
  private static final byte[] SINK = new byte[64 * 1024 * 1024];

  @State(Scope.Benchmark)
  public static class RandomReadParams {
    private final Random mRandom = new Random(SEED);

    @Param({"16", "64"})
    public long mBlockSizeMB;

    public long mBlockSize;

    public final long mLocalBlockId = 1L;

    public final long mUfsMountId = 2L;

    public final long mUfsBlockId = 3L;

    public String mUfsPath;

    /** Each random read's size: 1MB. */
    @Param({"1"})
    public long mReadSizeMB;

    public long mReadSize;

    public BlockStoreBase mBlockStoreBase;

    /** Sequence of random offsets. */
    public long[] mOffsets;

    /** Block Data. */
    public byte[] mData;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      mBlockSize = mBlockSizeMB * 1024 * 1024;
      mReadSize = mReadSizeMB * 1024 * 1024;

      int numReads = (int) (mBlockSize / mReadSize);
      // generate an array of offsets
      mOffsets = mRandom.longs(numReads, 0, mBlockSize - mReadSize).toArray();

      mBlockStoreBase = BlockStoreBase.create();

      mData = new byte[(int) mBlockSize];
      mRandom.nextBytes(mData);

      mBlockStoreBase.prepareLocalBlock(mLocalBlockId, mBlockSize, mData);

      String ufsRoot = AlluxioTestDirectory.createTemporaryDirectory("ufs").getAbsolutePath();
      mBlockStoreBase.mountUfs(mUfsMountId, ufsRoot);
      mUfsPath = PathUtils.concatUfsPath(ufsRoot, "test_file");
      mBlockStoreBase.prepareUfsFile(mUfsPath, mData);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
      mBlockStoreBase.mMonoBlockStore.removeBlock(1L, mLocalBlockId);
      mBlockStoreBase.close();
    }
  }

  @Benchmark
  public void monoBlockStoreRandReadLocal(RandomReadParams params) throws Exception {
    randReadLocal(params.mBlockStoreBase.mMonoBlockStore,
        params.mLocalBlockId, params.mBlockSize, params.mOffsets, params.mReadSize);
  }

  @Benchmark
  public void monoBlockStoreRandTransferLocal(RandomReadParams params) throws Exception {
    randTransferLocal(params.mBlockStoreBase.mMonoBlockStore,
        params.mLocalBlockId, params.mBlockSize, params.mOffsets, params.mReadSize);
  }

  @Benchmark
  public void monoBlockStoreRandReadUfs(RandomReadParams params) throws Exception {
    randReadUfs(params.mBlockStoreBase.mMonoBlockStore, params.mUfsBlockId,
        params.mUfsMountId, params.mUfsPath, params.mBlockSize, params.mOffsets, params.mReadSize);
  }

  @Benchmark
  public void pagedBlockStoreRandReadLocal(RandomReadParams params) throws Exception {
    randReadLocal(params.mBlockStoreBase.mPagedBlockStore,
        params.mLocalBlockId, params.mBlockSize, params.mOffsets, params.mReadSize);
  }

  @Benchmark
  public void pagedBlockStoreRandTransferLocal(RandomReadParams params) throws Exception {
    randTransferLocal(params.mBlockStoreBase.mPagedBlockStore,
        params.mLocalBlockId, params.mBlockSize, params.mOffsets, params.mReadSize);
  }

  @Benchmark
  public void pagedBlockStoreRandReadUfs(RandomReadParams params) throws Exception {
    randReadUfs(params.mBlockStoreBase.mPagedBlockStore, params.mUfsBlockId,
        params.mUfsMountId, params.mUfsPath, params.mBlockSize, params.mOffsets, params.mReadSize);
  }

  private void randReadLocal(BlockStore store, long blockId,
                             long blockSize, long[] offsets, long readSize) throws IOException {
    try (BlockReader reader = store.createBlockReader(1L, blockId, 0, false,
           Protocol.OpenUfsBlockOptions.newBuilder().build())) {
      for (long offset: offsets) {
        ByteBuffer buffer = reader.read(offset, readSize);
        ByteBuf buf = Unpooled.wrappedBuffer(buffer);
        buf.readBytes(SINK, (int) offset, (int) readSize);
        buf.release();
      }
    }
  }

  private void randTransferLocal(BlockStore store, long blockId,
                                 long blockSize, long[] offsets, long readSize) throws IOException {

    try (BlockReader reader = store.createBlockReader(1L, blockId, 0, false,
            Protocol.OpenUfsBlockOptions.newBuilder().build())) {
      for (long offset: offsets) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer((int) readSize, (int) readSize);
        ((FileChannel) reader.getChannel()).position(offset);
        while (buf.writableBytes() > 0 && reader.transferTo(buf) >= 0) {}
        buf.readBytes(SINK, (int) offset, (int) readSize);
        buf.release();
      }
    }
  }

  private void randReadUfs(BlockStore store, long blockId, long mountId, String ufsPath,
                             long blockSize, long[] offsets, long readSize) throws IOException {
    try (BlockReader reader = store.createBlockReader(1L, blockId, 0, false,
        Protocol.OpenUfsBlockOptions
            .newBuilder()
            .setNoCache(true)
            .setMountId(mountId)
            .setUfsPath(ufsPath)
            .setBlockSize(blockSize)
            .build())) {
      for (long offset: offsets) {
        ByteBuffer buffer = reader.read(offset, readSize);
        ByteBuf buf = Unpooled.wrappedBuffer(buffer);
        buf.readBytes(SINK, (int) offset, (int) readSize);
        buf.release();
      }
    }
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
        .parent(argsCli)
        .addProfiler(StackProfiler.class)
        .include(BlockStoreRandomReadBench.class.getName())
        .result("results.json")
        .resultFormat(ResultFormatType.JSON)
        .shouldDoGC(true)
        .build();
    new Runner(opts).run();
  }
}
