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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.block.BlockMaster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.allocator.GreedyAllocator;
import alluxio.worker.block.meta.BlockMeta;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;

/**
 * Integration tests for {@link alluxio.worker.block.meta.StorageTier}.
 */
public class TieredStoreIntegrationTest extends BaseIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 1000;
  private static final WaitForOptions WAIT_OPTIONS =
      WaitForOptions.defaults().setTimeoutMs(2000).setInterval(10);

  private FileSystem mFileSystem;
  private BlockMaster mBlockMaster;
  private SetAttributePOptions mSetPinned;
  private SetAttributePOptions mSetUnpinned;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, MEM_CAPACITY_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, 1000)
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
    mBlockMaster = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(BlockMaster.class);
    mSetPinned = SetAttributePOptions.newBuilder().setPinned(true).build();
    mSetUnpinned = SetAttributePOptions.newBuilder().setPinned(false).build();
  }

  /**
   * Tests that deletes go through despite failing initially due to concurrent read.
   */
  @Test
  public void deleteWhileRead() throws Exception {
    int fileSize = MEM_CAPACITY_BYTES / 2; // Small enough not to trigger async eviction

    AlluxioURI file = new AlluxioURI("/test1");
    FileSystemTestUtils.createByteFile(mFileSystem, file, WritePType.MUST_CACHE, fileSize);

    CommonUtils.waitFor("file in memory", () -> {
      try {
        return 100 == mFileSystem.getStatus(file).getInAlluxioPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);

    // Open the file
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build();
    FileInStream in = mFileSystem.openFile(file, options);
    Assert.assertEquals(0, in.read());

    // Delete the file
    mFileSystem.delete(file);

    // After the delete, the master should no longer serve the file
    Assert.assertFalse(mFileSystem.exists(file));

    // However, the previous read should still be able to read it as the data still exists
    byte[] res = new byte[fileSize];
    Assert.assertEquals(fileSize - 1, in.read(res, 1, fileSize - 1));
    res[0] = 0;
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(fileSize, res));
    in.close();

    CommonUtils.waitFor("file blocks are deleted" , () -> 0 == mBlockMaster.getUsedBytes(),
        WAIT_OPTIONS);

    // After the file is closed, the master's delete should go through and new files can be created
    AlluxioURI newFile = new AlluxioURI("/test2");
    FileSystemTestUtils.createByteFile(mFileSystem, newFile, WritePType.MUST_CACHE,
        MEM_CAPACITY_BYTES);
    Assert.assertEquals(100, mFileSystem.getStatus(newFile).getInAlluxioPercentage());
  }

  /**
   * Tests that pinning a file prevents it from being evicted.
   */
  @Test
  public void pinFile() throws Exception {
    // Create a file that fills the entire Alluxio store
    AlluxioURI file = new AlluxioURI("/test1");
    // Half of mem capacity to avoid triggering async eviction
    FileSystemTestUtils.createByteFile(mFileSystem, file, WritePType.MUST_CACHE,
        MEM_CAPACITY_BYTES / 2);

    // Pin the file
    mFileSystem.setAttribute(file, mSetPinned);

    // Confirm the pin with master
    Assert.assertTrue(mFileSystem.getStatus(file).isPinned());
    // Try to create a file that cannot be stored unless the previous file is evicted, expect an
    // exception since worker cannot serve the request
    mThrown.expect(Exception.class);
    FileSystemTestUtils.createByteFile(mFileSystem, "/test2", WritePType.MUST_CACHE,
        MEM_CAPACITY_BYTES);
  }

  /**
   * Tests that pinning a file and then unpinning.
   */
  @Test
  public void unpinFile() throws Exception {
    // Create a file that fills the entire Alluxio store
    AlluxioURI file1 = new AlluxioURI("/test1");
    FileSystemTestUtils
        .createByteFile(mFileSystem, file1, WritePType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // Pin the file
    mFileSystem.setAttribute(file1, mSetPinned);
    Assert.assertTrue(mFileSystem.getStatus(file1).isPinned());

    // Unpin the file
    mFileSystem.setAttribute(file1, mSetUnpinned);
    // Confirm the unpin
    Assert.assertFalse(mFileSystem.getStatus(file1).isPinned());

    // Wait until worker receives the new pin-list.
    Thread.sleep(2 * Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));

    // Try to create a file that cannot be stored unless the previous file is evicted, this
    // should succeed
    AlluxioURI file2 = new AlluxioURI("/test2");
    FileSystemTestUtils
        .createByteFile(mFileSystem, file2, WritePType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // Wait for validation.
    CommonUtils.waitFor("file2 should be in memory and file1 should be evicted", () -> {
      try {
        return 0 == mFileSystem.getStatus(file1).getInAlluxioPercentage()
            && 100 == mFileSystem.getStatus(file2).getInAlluxioPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
  }

  /**
   * Tests the promotion of a file.
   */
  @Test
  public void promoteBlock() throws Exception {
    AlluxioURI uri1 = new AlluxioURI("/file1");
    AlluxioURI uri2 = new AlluxioURI("/file2");
    AlluxioURI uri3 = new AlluxioURI("/file3");
    FileSystemTestUtils.createByteFile(mFileSystem, uri1, WritePType.CACHE_THROUGH,
        MEM_CAPACITY_BYTES / 3);
    FileSystemTestUtils.createByteFile(mFileSystem, uri2, WritePType.CACHE_THROUGH,
        MEM_CAPACITY_BYTES / 2);
    FileSystemTestUtils.createByteFile(mFileSystem, uri3, WritePType.CACHE_THROUGH,
        MEM_CAPACITY_BYTES / 2);

    AlluxioURI toPromote;
    int toPromoteLen;
    URIStatus file1Info = mFileSystem.getStatus(uri1);
    URIStatus file2Info = mFileSystem.getStatus(uri2);
    URIStatus file3Info = mFileSystem.getStatus(uri3);

    // We know some file will not be in memory, but not which one since we do not want to make
    // any assumptions on the eviction policy
    if (file1Info.getInAlluxioPercentage() < 100) {
      toPromote = uri1;
      toPromoteLen = (int) file1Info.getLength();
      Assert.assertEquals(100, file2Info.getInAlluxioPercentage());
      Assert.assertEquals(100, file3Info.getInAlluxioPercentage());
    } else if (file2Info.getInMemoryPercentage() < 100) {
      toPromote = uri2;
      toPromoteLen = (int) file2Info.getLength();
      Assert.assertEquals(100, file1Info.getInAlluxioPercentage());
      Assert.assertEquals(100, file3Info.getInAlluxioPercentage());
    } else {
      toPromote = uri3;
      toPromoteLen = (int) file3Info.getLength();
      Assert.assertEquals(100, file1Info.getInAlluxioPercentage());
      Assert.assertEquals(100, file2Info.getInAlluxioPercentage());
    }

    FileInStream is = mFileSystem.openFile(toPromote,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build());
    byte[] buf = new byte[toPromoteLen];
    int len = is.read(buf);
    is.close();

    Assert.assertEquals(toPromoteLen, len);
    CommonUtils.waitFor("file in memory", () -> {
      try {
        return 100 == mFileSystem.getStatus(toPromote).getInAlluxioPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
  }

  /**
   * With setting of "StorageDir1, StorageDir2", loading a block from UFS that is bigger than
   * the available space of StorageDir1 should land in StorageDir2 using GreedyAllocator.
   * https://github.com/Alluxio/alluxio/issues/8687
   */
  @Test
  public void greedyAllocator() throws Exception {
    mLocalAlluxioClusterResource.stop();
    int fileLen = 8 * Constants.MB;
    mLocalAlluxioClusterResource
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "4MB")
        .setProperty(PropertyKey.WORKER_ALLOCATOR_CLASS, GreedyAllocator.class.getName())
        .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVELS, 1)
        .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, ImmutableList.of(
            mTempFolder.newFolder("dir1").getAbsolutePath(),
            mTempFolder.newFolder("dir2").getAbsolutePath()))
        .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
            ImmutableList.of("2MB", String.valueOf(2 * fileLen)));
    mLocalAlluxioClusterResource.start();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    AlluxioURI uri1 = new AlluxioURI("/file1");
    FileSystemTestUtils.createByteFile(mFileSystem, uri1, WritePType.THROUGH, fileLen);
    Assert.assertEquals(0, mFileSystem.getStatus(uri1).getInAlluxioPercentage());
    FileSystemTestUtils.loadFile(mFileSystem, uri1.getPath());
    CommonUtils.waitFor("file is in Alluxio", () -> {
      try {
        return 100 == mFileSystem.getStatus(uri1).getInAlluxioPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
  }

  /**
   * Tests that when a corrupt block is being read, tiered store will remove this block
   * from both block meta and the physical block file, and falls back to read it from the UFS.
   * A corrupt block is defined as:
   * 1. its block meta exists in memory, but no physical block file exist in the cache directory, or
   * 2. its length in block meta is non-zero, but the physical block file is 0-sized.
   */
  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "100",
      // disable short circuit read to ensure blocks are read from worker block store
      PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED, "false",
  })
  public void removesCorruptBlockAndFallbackToUfs() throws Exception {
    BlockWorker worker = mLocalAlluxioClusterResource.get()
        .getWorkerProcess()
        .getWorker(BlockWorker.class);
    AlluxioURI file = new AlluxioURI("/file1");
    int fileLen = 3 * 100; // 3 blocks, 100 bytes each
    prepareCorruptThreeBlockFile(file, fileLen);
    URIStatus fileStatus = mFileSystem.getStatus(file);
    List<FileBlockInfo> blocks = fileStatus.getFileBlockInfos();
    // verify that the file can be read
    FileInStream is = mFileSystem.openFile(fileStatus, FileSystemOptionsUtils.openFileDefaults(
        mLocalAlluxioClusterResource.get().getClient().getConf())
            .toBuilder()
            .setReadType(ReadPType.NO_CACHE) // don't cache the corrupt block
            .build());
    byte[] fileContent = ByteStreams.toByteArray(is);
    Assert.assertTrue(
        BufferUtils.equalIncreasingByteArray(fileLen, fileContent));
    Assert.assertFalse(worker
        .getBlockStore()
        .getVolatileBlockMeta(blocks.get(0).getBlockInfo().getBlockId())
        .isPresent());
    Assert.assertFalse(worker
        .getBlockStore()
        .getVolatileBlockMeta(blocks.get(1).getBlockInfo().getBlockId())
        .isPresent());
    Assert.assertTrue(worker
        .getBlockStore()
        .getVolatileBlockMeta(blocks.get(2).getBlockInfo().getBlockId())
        .isPresent());
    // verify that the block location info has been updated in master
    Thread.sleep(mLocalAlluxioClusterResource.get().getClient().getConf()
        .getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2);
    URIStatus newStatus = mFileSystem.getStatus(file);
    Assert.assertEquals(0,
        newStatus.getFileBlockInfos().get(0).getBlockInfo().getLocations().size());
    Assert.assertEquals(0,
        newStatus.getFileBlockInfos().get(1).getBlockInfo().getLocations().size());
    Assert.assertEquals(1,
        newStatus.getFileBlockInfos().get(2).getBlockInfo().getLocations().size());
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "100",
      PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED, "true",
  })
  public void removesCorruptBlockAndFallbackToUfsShortCircuit() throws Exception {
    BlockWorker worker = mLocalAlluxioClusterResource.get()
        .getWorkerProcess()
        .getWorker(BlockWorker.class);
    AlluxioURI file = new AlluxioURI("/file1");
    int fileLen = 3 * 100; // 3 blocks, 100 bytes each
    prepareCorruptThreeBlockFile(file, fileLen);
    URIStatus fileStatus = mFileSystem.getStatus(file);
    List<FileBlockInfo> blocks = fileStatus.getFileBlockInfos();

    // verify that the block cannot be read via short-circuit
    FileSystemContext fsContext = FileSystemContext.create();
    InStreamOptions inStreamOptions = new InStreamOptions(fileStatus, fsContext.getClusterConf());
    Assert.assertThrows(NotFoundException.class, () -> new LocalFileDataReader.Factory(
        fsContext, worker.getWorkerAddress(), blocks.get(0).getBlockInfo().getBlockId(),
        Constants.KB, inStreamOptions));

    // verify that the file is readable
    FileInStream is = mFileSystem.openFile(fileStatus, FileSystemOptionsUtils.openFileDefaults(
            fsContext.getClusterConf())
        .toBuilder()
        .setReadType(ReadPType.NO_CACHE) // don't cache the corrupt block
        .build());
    byte[] fileContent = ByteStreams.toByteArray(is);
    Assert.assertTrue(
        BufferUtils.equalIncreasingByteArray(fileLen, fileContent));
    Assert.assertFalse(worker
        .getBlockStore()
        .getVolatileBlockMeta(blocks.get(0).getBlockInfo().getBlockId())
        .isPresent());
    Assert.assertFalse(worker
        .getBlockStore()
        .getVolatileBlockMeta(blocks.get(1).getBlockInfo().getBlockId())
        .isPresent());
    Assert.assertTrue(worker
        .getBlockStore()
        .getVolatileBlockMeta(blocks.get(2).getBlockInfo().getBlockId())
        .isPresent());
    // verify that the block location info has been updated in master
    Thread.sleep(fsContext
        .getClusterConf()
        .getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2);
    URIStatus newStatus = mFileSystem.getStatus(file);
    Assert.assertEquals(0,
        newStatus.getFileBlockInfos().get(0).getBlockInfo().getLocations().size());
    Assert.assertEquals(0,
        newStatus.getFileBlockInfos().get(1).getBlockInfo().getLocations().size());
    Assert.assertEquals(1,
        newStatus.getFileBlockInfos().get(2).getBlockInfo().getLocations().size());
  }

  /**
   * Prepares a 3-block file, truncates the first block to 0 size, removes the second block, and
   * leaves the third block intact.
   */
  private void prepareCorruptThreeBlockFile(AlluxioURI fileName, int fileLen) throws Exception {
    BlockWorker worker = mLocalAlluxioClusterResource.get()
        .getWorkerProcess()
        .getWorker(BlockWorker.class);
    FileSystemTestUtils.createByteFile(mFileSystem, fileName, WritePType.CACHE_THROUGH, fileLen);
    URIStatus fileStatus = mFileSystem.getStatus(fileName);
    Path ufsFilePath = Paths.get(fileStatus.getFileInfo().getUfsPath());
    Assert.assertTrue(Files.exists(ufsFilePath));
    Assert.assertEquals(fileLen, Files.size(ufsFilePath));

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
}
