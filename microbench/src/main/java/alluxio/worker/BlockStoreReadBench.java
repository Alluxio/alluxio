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
import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

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
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 2, time = 3)
@BenchmarkMode(Mode.Throughput)
public class BlockStoreReadBench {
  private static final byte[] SINK = new byte[64 * 1024 * 1024];

  @State(Scope.Benchmark)
  public static class BenchParams {
    private final Random mRandom = new Random();

    @Param({"16", "64"})
    public long mBlockSizeMB;

    private long mBlockSizeByte;

    BlockStoreBase mBlockStoreBase;

    // Local Block Id that has been cached
    final long mLocalBlockId = 1L;

    // ufs mount id
    final long mUfsMountId = 10L;
    // ufs file path that is not cached yet
    String mUfsPath;
    // ufs block id
    long mUfsBlockId = 3L;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      mBlockSizeByte = mBlockSizeMB * 1024L * 1024L;
      mBlockStoreBase = BlockStoreBase.create();

      // prepare some random data
      byte[] data = new byte[(int) mBlockSizeByte];
      mRandom.nextBytes(data);

      prepareLocalBlock(data);

      // ufs block is used by both Mono and Paged block store
      prepareUfsBlock(data);
    }

    @Setup(Level.Invocation)
    public void cleanUpUfs() throws Exception {
      mBlockStoreBase.mMonoBlockStore.removeBlock(1L, mUfsBlockId);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
      mBlockStoreBase.close();
      // todo(yangchen): remove block from PagedBlockStore too
    }

    private void prepareLocalBlock(byte[] data) throws Exception {
      // create local block
      mBlockStoreBase.mMonoBlockStore
              .createBlock(1, mLocalBlockId, 0,
                      new CreateBlockOptions(null, null, mBlockSizeByte));
      try (BlockWriter writer = mBlockStoreBase.mMonoBlockStore.createBlockWriter(1, mLocalBlockId)) {
        writer.append(ByteBuffer.wrap(data));
      }
      mBlockStoreBase.mMonoBlockStore.commitBlock(1, mLocalBlockId, false);
    }

    private void prepareUfsBlock(byte[] data) throws Exception {
      // set up ufs root
      File ufsRoot = AlluxioTestDirectory.createTemporaryDirectory("ufs");
      mBlockStoreBase.mUfsManager.addMount(
              mUfsMountId, new AlluxioURI(ufsRoot.getAbsolutePath()),
              UnderFileSystemConfiguration.defaults(Configuration.global()));

      // create ufs block file
      mUfsPath = PathUtils.concatUfsPath(ufsRoot.getAbsolutePath(), "file1");
      File ufsFile = new File(mUfsPath);
      if (!ufsFile.createNewFile()) {
        throw new RuntimeException("Failed to create ufs file");
      }
      try (FileOutputStream out = new FileOutputStream(ufsFile);
           BufferedOutputStream bout = new BufferedOutputStream(out)) {
        bout.write(data);
        bout.flush();
      }
    }
  }

  @Benchmark
  public void monoBlockStoreReadLocal(BenchParams params) throws Exception {
    ByteBuffer buf = readFullyLocal(params.mBlockStoreBase.mMonoBlockStore, params.mLocalBlockId, params.mBlockSizeByte);
    buf.get(SINK, 0, (int) params.mBlockSizeByte);
  }

  private ByteBuffer readFullyLocal(BlockStore store, long blockId, long blockSize) throws Exception {
    try (BlockReader reader = store
        .createBlockReader(2L, blockId, 0, false,
            Protocol.OpenUfsBlockOptions.newBuilder().build())) {
      return reader.read(0, blockSize);
    }
  }

  @Benchmark
  public void monoBlockStoreReadUfs(BenchParams params, Blackhole bh) throws Exception {
    ByteBuffer buf = readFullyUfs(params.mBlockStoreBase.mMonoBlockStore, params.mUfsBlockId,
        params.mUfsMountId, params.mUfsPath, params.mBlockSizeByte);
    buf.get(SINK, 0, (int) params.mBlockSizeByte);
  }

  @Benchmark
  public void pagedBlockStoreReadUfs(BenchParams params, Blackhole bh) throws Exception {
    ByteBuffer buf = readFullyUfs(params.mBlockStoreBase.mPagedBlockStore, params.mUfsBlockId,
        params.mUfsMountId, params.mUfsPath, params.mBlockSizeByte);
    buf.get(SINK, 0, (int) params.mBlockSizeByte);
  }

  private ByteBuffer readFullyUfs(BlockStore store, long blockId, long mountId, String ufsPath, long blockSize)
      throws Exception {
    try (BlockReader reader = store
        .createBlockReader(2L, blockId, 0, false,
            Protocol.OpenUfsBlockOptions
                .newBuilder()
                    .setUfsPath(ufsPath)
                    .setMountId(mountId)
                    .setBlockSize(blockSize)
                    .build())) {
      return reader.read(0, blockSize);
    }
  }
}
