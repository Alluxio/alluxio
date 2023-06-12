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

package alluxio.server.tieredstore;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.stream.LocalFileDataReader;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.meta.BlockMeta;

import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Tests that when a corrupt block is being read, tiered store will remove this block
 * from both block meta and the physical block file, and falls back to read it from the UFS.
 * A corrupt block is defined as:
 * 1. its block meta exists in memory, but no physical block file exist in the cache directory, or
 * 2. its length in block meta is non-zero, but the physical block file is 0-sized.
 */
public class TieredStoreBlockCorruptionIntegrationTest extends BaseIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 1000;
  private static final int BLOCK_SIZE = 100;
  private final AlluxioURI mFile = new AlluxioURI("/file1");
  private final int mFileLength = 3 * 100; // 3 blocks, 100 bytes each
  private BlockWorker mWorker;
  private FileSystem mFileSystem;

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, MEM_CAPACITY_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, 100)
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO, 0.8)
          .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, 100)
          .setProperty(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, false)
          .setProperty(PropertyKey.WORKER_REVIEWER_CLASS,
              "alluxio.worker.block.reviewer.AcceptingReviewer")
          .build();

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWorker = mLocalAlluxioClusterResource.get()
        .getWorkerProcess()
        .getWorker(BlockWorker.class);
    prepareFileWithCorruptBlocks();
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED, "false",
  })
  public void sequentialRead() throws Exception {
    verifySequentialReadable();
    verifyBlockMetadata();
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED, "false",
  })
  public void positionedRead() throws Exception {
    verifyPositionedReadable();
    verifyBlockMetadata();
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED, "true",
  })
  public void shortCircuitRead() throws Exception {
    // verify that the block cannot be read via short-circuit
    try (FileSystemContext fsContext = FileSystemContext.create()) {
      URIStatus fileStatus = mFileSystem.getStatus(mFile);
      List<FileBlockInfo> blocks = fileStatus.getFileBlockInfos();
      InStreamOptions inStreamOptions = new InStreamOptions(fileStatus,
          fsContext.getClusterConf());
      Assert.assertThrows(NotFoundException.class, () -> new LocalFileDataReader.Factory(
          fsContext, mWorker.getWorkerAddress(), blocks.get(0).getBlockInfo().getBlockId(),
          Constants.KB, inStreamOptions));
    }

    verifySequentialReadable();
    verifyBlockMetadata();
  }

  /**
   * Prepares a 3-block file, truncates the first block to 0 size, removes the second block, and
   * leaves the third block intact.
   */
  private void prepareFileWithCorruptBlocks() throws Exception {
    BlockWorker worker = mLocalAlluxioClusterResource.get()
        .getWorkerProcess()
        .getWorker(BlockWorker.class);
    FileSystemTestUtils.createByteFile(mFileSystem, mFile, WritePType.CACHE_THROUGH, mFileLength);
    URIStatus fileStatus = mFileSystem.getStatus(mFile);
    Path ufsFilePath = Paths.get(fileStatus.getFileInfo().getUfsPath());
    Assert.assertTrue(Files.exists(ufsFilePath));
    Assert.assertEquals(mFileLength, Files.size(ufsFilePath));

    List<FileBlockInfo> blocks = fileStatus.getFileBlockInfos();
    Assert.assertEquals(3, blocks.size());
    Assert.assertTrue(blocks.get(0).getBlockInfo().getLocations().size() >= 1);
    Assert.assertTrue(blocks.get(1).getBlockInfo().getLocations().size() >= 1);
    Assert.assertTrue(blocks.get(2).getBlockInfo().getLocations().size() >= 1);

    // truncate the first block on disk, bypassing worker management to simulate block corruption
    Optional<BlockMeta> firstBlockMeta =
        worker.getBlockStore().getVolatileBlockMeta(blocks.get(0).getBlockInfo().getBlockId());
    Assert.assertTrue(
        String.format("Block meta of first block does not exist on worker %s",
            worker.getWorkerAddress()), firstBlockMeta.isPresent());
    Path blockFilePath = Paths.get(firstBlockMeta.get().getPath());
    Files.write(blockFilePath, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
    Assert.assertTrue(Files.exists(blockFilePath));
    Assert.assertEquals(0, Files.size(blockFilePath));

    // remove the second block file
    Optional<BlockMeta> secondBlockMeta =
        worker.getBlockStore().getVolatileBlockMeta(blocks.get(1).getBlockInfo().getBlockId());
    Assert.assertTrue(
        String.format("Block meta of second block does not exist on worker %s",
            worker.getWorkerAddress()), secondBlockMeta.isPresent());
    blockFilePath = Paths.get(secondBlockMeta.get().getPath());
    Files.deleteIfExists(blockFilePath);
    Assert.assertFalse(Files.exists(blockFilePath));

    Optional<BlockMeta> thirdBlockMeta =
        worker.getBlockStore().getVolatileBlockMeta(blocks.get(2).getBlockInfo().getBlockId());
    Assert.assertTrue(
        String.format("Block meta of third block does not exist on worker %s",
            worker.getWorkerAddress()), thirdBlockMeta.isPresent());
    blockFilePath = Paths.get(thirdBlockMeta.get().getPath());
    Assert.assertTrue(Files.exists(blockFilePath));
    Assert.assertEquals(thirdBlockMeta.get().getBlockSize(), Files.size(blockFilePath));
  }

  private void verifySequentialReadable() throws Exception {
    URIStatus fileStatus = mFileSystem.getStatus(mFile);
    try (FileInStream is = mFileSystem.openFile(
        fileStatus,
        FileSystemOptionsUtils.openFileDefaults(
            mLocalAlluxioClusterResource.get().getClient().getConf())
        .toBuilder()
        .setReadType(ReadPType.NO_CACHE) // don't cache the corrupt block
        .build())) {
      byte[] fileContent = ByteStreams.toByteArray(is);
      Assert.assertTrue(
          BufferUtils.equalIncreasingByteArray(mFileLength, fileContent));
    }
  }

  private void verifyPositionedReadable() throws Exception {
    URIStatus fileStatus = mFileSystem.getStatus(mFile);
    try (FileInStream is = mFileSystem.openFile(
        fileStatus,
        FileSystemOptionsUtils.openFileDefaults(
            mLocalAlluxioClusterResource.get().getClient().getConf())
        .toBuilder()
        .setReadType(ReadPType.NO_CACHE) // don't cache the corrupt block
        .build())) {
      final long startPos = 0;
      int totalBytesRead = 0;
      byte[] buffer = new byte[Constants.KB];
      int bytesRead = is.positionedRead(startPos, buffer, totalBytesRead, buffer.length);
      while (bytesRead != -1) {
        totalBytesRead += bytesRead;
        Assert.assertTrue(totalBytesRead <= mFileLength);
        bytesRead = is.positionedRead(
            startPos + totalBytesRead, buffer, totalBytesRead, buffer.length);
      }
      byte[] fileContent = Arrays.copyOfRange(buffer, 0, totalBytesRead);
      Assert.assertTrue(
          BufferUtils.equalIncreasingByteArray((int) startPos, totalBytesRead, fileContent));
    }
  }

  /**
   * Verifies that after corrupt blocks are detected and removed by tiered store, the first
   * two blocks are not cached by the worker, and their block location info from master does not
   * contain the worker.
   */
  private void verifyBlockMetadata() throws Exception {
    URIStatus fileStatus = mFileSystem.getStatus(mFile);
    List<FileBlockInfo> blocks = fileStatus.getFileBlockInfos();
    Assert.assertEquals(3, blocks.size());
    // verify that block meta are correct in block worker
    Assert.assertFalse(mWorker
        .getBlockStore()
        .getVolatileBlockMeta(blocks.get(0).getBlockInfo().getBlockId())
        .isPresent());
    Assert.assertFalse(mWorker
        .getBlockStore()
        .getVolatileBlockMeta(blocks.get(1).getBlockInfo().getBlockId())
        .isPresent());
    Assert.assertTrue(mWorker
        .getBlockStore()
        .getVolatileBlockMeta(blocks.get(2).getBlockInfo().getBlockId())
        .isPresent());

    // verify that the block location info has been updated in master after worker-master sync
    Thread.sleep(mLocalAlluxioClusterResource.get()
        .getClient()
        .getConf()
        .getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2);
    // retrieve latest block location info
    fileStatus = mFileSystem.getStatus(mFile);
    blocks = fileStatus.getFileBlockInfos();
    Assert.assertEquals(3, blocks.size());
    Assert.assertTrue(blocks
        .stream()
        .limit(2)
        .map(FileBlockInfo::getBlockInfo)
        .map(BlockInfo::getLocations)
        .flatMap(Collection::stream)
        .noneMatch(loc -> loc.getWorkerAddress().equals(mWorker.getWorkerAddress())));
    Assert.assertTrue(blocks
        .stream()
        .skip(2)
        .map(FileBlockInfo::getBlockInfo)
        .map(BlockInfo::getLocations)
        .flatMap(Collection::stream)
        .anyMatch(loc -> loc.getWorkerAddress().equals(mWorker.getWorkerAddress())));
  }
}
