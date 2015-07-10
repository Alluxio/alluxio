/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block.evictor;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.TestUtils;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreEventListener;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.io.LocalFileBlockWriter;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * This class provides utility methods for testing Evictors.
 */
class EvictorTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Default configurations of a TieredBlockStore for use in {@link #defaultMetadataManager}. They
   * represent a block store with a MEM tier and a SSD tier, there are two directories with capacity
   * 100 bytes and 200 bytes separately in the MEM tier and three directories with capacity 1000,
   * 2000, 3000 bytes separately in the SSD tier.
   */
  public static final int[] TIER_LEVEL = {0, 1};
  public static final String[] TIER_ALIAS = {"MEM", "SSD"};
  public static final String[][] TIER_PATH =
      { {"/mem/0", "/mem/1"}, {"/ssd/0", "/ssd/1", "/ssd/2"}};
  public static final long[][] TIER_CAPACITY = { {100, 200}, {1000, 2000, 3000}};

  /**
   * Create a {@link BlockMetadataManager} for a {@link tachyon.worker.block.TieredBlockStore}
   * configured by the parameters. For simplicity, you can use
   * {@link #defaultMetadataManager(String)} which calls this method with default values.
   *
   * @param tierLevel like {@link #TIER_LEVEL}, length must be > 0.
   * @param tierAlias like {@link #TIER_ALIAS}, each corresponds to an element in tierLevel
   * @param tierPath like {@link #TIER_PATH}, each list represents directories of the tier with the
   *        same list index in tierAlias
   * @param tierCapacity like {@link #TIER_CAPACITY}, should be in the same dimension with tierPath,
   *        each element is the capacity of the corresponding dir in tierPath
   * @return the created BlockMetadataManager
   * @throws IOException when initialization of TieredBlockStore fails
   */
  public static BlockMetadataManager newMetadataManager(int[] tierLevel, String[] tierAlias,
      String[][] tierPath, long[][] tierCapacity) throws IOException {
    // make sure dimensions are legal
    Preconditions.checkNotNull(tierLevel);
    Preconditions.checkNotNull(tierAlias);
    Preconditions.checkNotNull(tierPath);
    Preconditions.checkNotNull(tierCapacity);

    Preconditions.checkArgument(tierLevel.length > 0, "length of tierLevel should be > 0");
    Preconditions.checkArgument(tierLevel.length == tierAlias.length,
        "tierAlias and tierLevel should have the same length");
    Preconditions.checkArgument(tierLevel.length == tierPath.length,
        "tierPath and tierLevel should have the same length");
    Preconditions.checkArgument(tierLevel.length == tierCapacity.length,
        "tierCapacity and tierLevel should have the same length");
    int nTier = tierLevel.length;
    for (int i = 0; i < nTier; i ++) {
      Preconditions.checkArgument(tierPath[i].length == tierCapacity[i].length,
          String.format("tierPath[%d] and tierCapacity[%d] should have the same length", i, i));
    }

    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, String.valueOf(nTier));
    for (int i = 0; i < nTier; i ++) {
      int level = tierLevel[i];
      tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, level),
          tierAlias[i]);

      StringBuilder sb = new StringBuilder();
      for (String path : tierPath[i]) {
        sb.append(path);
        sb.append(",");
      }
      tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, level),
          sb.toString());

      sb = new StringBuilder();
      for (long capacity : tierCapacity[i]) {
        sb.append(capacity);
        sb.append(",");
      }
      tachyonConf.set(
          String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, level),
          sb.toString());
    }

    return BlockMetadataManager.newBlockMetadataManager(tachyonConf);
  }

  /**
   * Create a BlockMetadataManager by calling
   * {@link #newMetadataManager(int[], String[], String[][], long[][])} with default values:
   * {@link #TIER_LEVEL}, {@link #TIER_ALIAS}, {@link #TIER_PATH} with the baseDir as path prefix,
   * {@link #TIER_CAPACITY}.
   *
   * @param baseDir the directory path as prefix for paths of directories in the tiered storage. The
   *        directory needs to exist before calling this method.
   * @return the created metadata manager
   * @throws IOException when error happens during creating temporary folder
   */
  public static BlockMetadataManager defaultMetadataManager(String baseDir) throws IOException {
    String[][] dirs = new String[TIER_PATH.length][];
    for (int i = 0; i < TIER_PATH.length; i ++) {
      int len = TIER_PATH[i].length;
      dirs[i] = new String[len];
      for (int j = 0; j < len; j ++) {
        dirs[i][j] = CommonUtils.concatPath(baseDir, TIER_PATH[i][j]);
        FileUtils.forceMkdir(new File(dirs[i][j]));
      }
    }
    return newMetadataManager(TIER_LEVEL, TIER_ALIAS, dirs, TIER_CAPACITY);
  }

  /**
   * Cache bytes into StroageDir.
   *
   * @param userId user who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param dir the StorageDir the block resides in
   * @param meta the metadata manager to update meta of the block
   * @param evictor the evictor to be informed of the new block
   * @throws IOException when fail to cache
   */
  public static void cache(long userId, long blockId, long bytes, StorageDir dir,
      BlockMetadataManager meta, Evictor evictor) throws IOException {
    // prepare temp block
    TempBlockMeta block = new TempBlockMeta(userId, blockId, bytes, dir);
    meta.addTempBlockMeta(block);

    // write data
    File tempFile = new File(block.getPath());
    if (!tempFile.getParentFile().mkdir()) {
      throw new IOException(String.format(
          "Parent directory of %s can not be created for temp block", block.getPath()));
    }
    BlockWriter writer = new LocalFileBlockWriter(block);
    writer.append(TestUtils.getIncreasingByteBuffer(Ints.checkedCast(bytes)));
    writer.close();

    // commit block
    Files.move(tempFile, new File(block.getCommitPath()));
    meta.commitTempBlockMeta(block);

    // update evictor
    if (evictor instanceof BlockStoreEventListener) {
      ((BlockStoreEventListener) evictor)
          .onCommitBlock(userId, blockId, dir.toBlockStoreLocation());
    }
  }

  /**
   * Whether the plan can satisfy the requested free bytes to be available, assume all blocks in the
   * plan are in the same dir.
   *
   * @param bytesToBeAvailable the requested bytes to be available
   * @param plan the eviction plan, should not be null
   * @param meta the metadata manager
   * @return true if the request can be satisfied otherwise false
   * @throws IOException if can not get meta data of a block
   */
  public static boolean requestSpaceSatisfied(long bytesToBeAvailable, EvictionPlan plan,
      BlockMetadataManager meta) throws IOException {
    Preconditions.checkNotNull(plan);

    List<Long> blockIds = plan.toEvict();
    for (Pair<Long, BlockStoreLocation> move : plan.toMove()) {
      blockIds.add(move.getFirst());
    }

    long evictedOrMovedBytes = 0;
    for (long blockId : blockIds) {
      evictedOrMovedBytes += meta.getBlockMeta(blockId).getBlockSize();
    }

    BlockStoreLocation location =
        meta.getBlockMeta(blockIds.get(0)).getParentDir().toBlockStoreLocation();
    return (meta.getAvailableBytes(location) + evictedOrMovedBytes) >= bytesToBeAvailable;
  }

  /**
   * Whether blocks in the EvictionPlan are in the same StorageDir.
   *
   * @param plan the eviction plan
   * @param meta the meta data manager
   * @return true if blocks are in the same dir otherwise false
   * @throws IOException if fail to get meta data of a block
   */
  public static boolean blocksInTheSameDir(EvictionPlan plan, BlockMetadataManager meta)
      throws IOException {
    Preconditions.checkNotNull(plan);

    StorageDir dir = null;
    List<Long> blockIds = plan.toEvict();
    for (Pair<Long, BlockStoreLocation> move : plan.toMove()) {
      blockIds.add(move.getFirst());
    }

    for (long blockId : blockIds) {
      StorageDir blockDir = meta.getBlockMeta(blockId).getParentDir();
      if (dir == null) {
        dir = blockDir;
      } else if (dir != blockDir) {
        return false;
      }
    }
    return true;
  }

  /**
   * Assume the plan is returned by a non-cascading evictor, check whether it is legal. a cascading
   * evictor is an evictor that always tries to move from the target tier to the next tier and
   * recursively move down 1 tier until finally blocks are evicted from the final tier.
   *
   * @param bytesToBeAvailable the requested bytes to be available
   * @param plan the eviction plan, should not be null
   * @param metaManager the meta data manager
   * @return true if and only if the plan is not null and both {@link #blocksInTheSameDir} and
   *         {@link #requestSpaceSatisfied} are true, otherwise false
   * @throws IOException when fail to get meta data of a block
   */
  public static boolean legalNonCascadingPlan(long bytesToBeAvailable, EvictionPlan plan,
      BlockMetadataManager metaManager) throws IOException {
    Preconditions.checkNotNull(plan);
    return blocksInTheSameDir(plan, metaManager)
        && requestSpaceSatisfied(bytesToBeAvailable, plan, metaManager);
  }

  /**
   * Assume the plan is returned by a cascading evictor, check whether it is legal. for explaination
   * of cascading evictor, please refer to {@link #legalNonCascadingPlan}.
   *
   * @param bytesToBeAvailable the requested bytes to be available
   * @param plan the eviction plan, should not be null
   * @param metaManager the meta data manager
   * @return true
   * @throws IOException when fail to get meta data of a block
   */
  public static boolean legalCascadingPlan(long bytesToBeAvailable, EvictionPlan plan,
      BlockMetadataManager metaManager) throws IOException {
    return EvictorUtils.legalCascadingPlan(bytesToBeAvailable, plan, metaManager);
  }

  /**
   * Only when plan is not null and at least one of {@link #legalCascadingPlan},
   * {@link #legalNonCascadingPlan} is true, the assertion will be passed, used in unit test.
   *
   * @param bytesToBeAvailable the requested bytes to be available
   * @param plan the eviction plan, should not be null
   * @param metaManager the meta data manager
   * @throws IOException when fail to get meta data of a block
   */
  public static void assertLegalPlan(long bytesToBeAvailable, EvictionPlan plan,
      BlockMetadataManager metaManager) throws IOException {
    Assert.assertNotNull(plan);
    Assert.assertTrue(legalNonCascadingPlan(bytesToBeAvailable, plan, metaManager)
        || legalCascadingPlan(bytesToBeAvailable, plan, metaManager));
  }
}
