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

package tachyon.worker.block;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.conf.TachyonConf;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.FileUtils;
import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.evictor.Evictor;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.io.LocalFileBlockWriter;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * This class provides utility methods for testing tiered block store.
 */
public class TieredBlockStoreTestUtils {
  /**
   * Default configurations of a TieredBlockStore for use in {@link #defaultMetadataManager}. They
   * represent a block store with a MEM tier and a SSD tier, there are two directories with capacity
   * 100 bytes and 200 bytes separately in the MEM tier and three directories with capacity 1000,
   * 2000, 3000 bytes separately in the SSD tier.
   */
  public static final int[] TIER_LEVEL = {0, 1};
  public static final StorageLevelAlias[] TIER_ALIAS = {StorageLevelAlias.MEM,
      StorageLevelAlias.SSD};
  public static final String[][] TIER_PATH =
      { {"/mem/0", "/mem/1"}, {"/ssd/0", "/ssd/1", "/ssd/2"}};
  public static final long[][] TIER_CAPACITY = { {2000, 3000}, {10000, 20000, 30000}};

  /**
   * Create a {@link TachyonConf} for a {@link TieredBlockStore} configured by the parameters. For
   * simplicity, you can use {@link #defaultTachyonConf(String)} which calls this method with
   * default values.
   *
   * @param tierLevel like {@link #TIER_LEVEL}, length must be &gt; 0.
   * @param tierAlias like {@link #TIER_ALIAS}, each corresponds to an element in tierLevel
   * @param tierPath like {@link #TIER_PATH}, each list represents directories of the tier with the
   *        same list index in tierAlias
   * @param tierCapacity like {@link #TIER_CAPACITY}, should be in the same dimension with tierPath,
   *        each element is the capacity of the corresponding dir in tierPath
   * @return the created TachyonConf
   */
  public static TachyonConf newTachyonConf(int[] tierLevel, StorageLevelAlias[] tierAlias,
      String[][] tierPath, long[][] tierCapacity) {
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
          tierAlias[i].toString());

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
    return tachyonConf;
  }

  /**
   * Create a BlockMetadataManager with {@link #defaultTachyonConf}.
   *
   * @param baseDir the directory path as prefix for paths of directories in the tiered storage. The
   *        directory needs to exist before calling this method.
   * @return the created metadata manager
   * @throws Exception when error happens during creating temporary folder
   */
  public static BlockMetadataManager defaultMetadataManager(String baseDir) throws Exception {
    TachyonConf tachyonConf = WorkerContext.getConf();
    tachyonConf.merge(defaultTachyonConf(baseDir));
    return BlockMetadataManager.newBlockMetadataManager();
  }

  /**
   * Create a {@link TachyonConf} with default values of {@link #TIER_LEVEL}, {@link #TIER_ALIAS},
   * {@link #TIER_PATH} with the baseDir as path prefix, {@link #TIER_CAPACITY}.
   *
   * @param baseDir the directory path as prefix for paths of directories in the tiered storage. The
   *        directory needs to exist before calling this method.
   * @return the created metadata manager
   * @throws Exception when error happens during creating temporary folder
   */
  public static TachyonConf defaultTachyonConf(String baseDir) throws Exception {
    String[][] dirs = new String[TIER_PATH.length][];
    for (int i = 0; i < TIER_PATH.length; i ++) {
      int len = TIER_PATH[i].length;
      dirs[i] = new String[len];
      for (int j = 0; j < len; j ++) {
        dirs[i][j] = PathUtils.concatPath(baseDir, TIER_PATH[i][j]);
        FileUtils.createDir(dirs[i][j]);
      }
    }
    return newTachyonConf(TIER_LEVEL, TIER_ALIAS, dirs, TIER_CAPACITY);
  }

  /**
   * Cache bytes into StorageDir.
   *
   * @param userId user who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param dir the StorageDir the block resides in
   * @param meta the metadata manager to update meta of the block
   * @param evictor the evictor to be informed of the new block
   * @throws Exception when fail to cache
   */
  public static void cache(long userId, long blockId, long bytes, StorageDir dir,
      BlockMetadataManager meta, Evictor evictor) throws Exception {
    TempBlockMeta tempBlockMeta = createTempBlock(userId, blockId, bytes, dir);

    // commit block
    FileUtils.move(tempBlockMeta.getPath(), tempBlockMeta.getCommitPath());
    meta.commitTempBlockMeta(tempBlockMeta);

    // update evictor
    if (evictor instanceof BlockStoreEventListener) {
      ((BlockStoreEventListener) evictor)
          .onCommitBlock(userId, blockId, dir.toBlockStoreLocation());
    }
  }

  /**
   * Cache bytes into StorageDir.
   *
   * @param tierLevel tier level of the StorageDir the block resides in
   * @param dirIndex index of directory in the tierLevel the block resides in
   * @param meta the metadata manager to update meta of the block
   * @param evictor the evictor to be informed of the new block
   * @throws Exception when fail to cache
   */
  public static void cache(long userId, long blockId, long bytes, int tierLevel, int dirIndex,
      BlockMetadataManager meta, Evictor evictor) throws Exception {
    StorageDir dir = meta.getTiers().get(tierLevel).getDir(dirIndex);
    cache(userId, blockId, bytes, dir, meta, evictor);
  }

  /**
   * Make a temp block of a given size in StorageDir.
   *
   * @param userId user who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param dir the StorageDir the block resides in
   * @return the temp block meta
   * @throws Exception when fail to create this block
   */
  public static TempBlockMeta createTempBlock(long userId, long blockId, long bytes, StorageDir dir)
      throws Exception {
    // prepare temp block
    TempBlockMeta tempBlockMeta = new TempBlockMeta(userId, blockId, bytes, dir);
    dir.addTempBlockMeta(tempBlockMeta);

    // write data
    FileUtils.createFile(tempBlockMeta.getPath());
    BlockWriter writer = new LocalFileBlockWriter(tempBlockMeta);
    writer.append(BufferUtils.getIncreasingByteBuffer(Ints.checkedCast(bytes)));
    writer.close();
    return tempBlockMeta;
  }

  /**
   * Get the total capacity of all tiers in bytes.
   *
   * @return total capacity of all tiers in bytes
   */
  public static long getDefaultTotalCapacityBytes() {
    long totalCapacity = 0;
    for (int i = 0; i < TIER_CAPACITY.length; i ++) {
      for (int j = 0; j < TIER_CAPACITY[i].length; j ++) {
        totalCapacity += TIER_CAPACITY[i][j];
      }
    }
    return totalCapacity;
  }

  /**
   * Get the number of testing directories of all tiers.
   *
   * @return number of testing directories of all tiers.
   */
  public static long getDefaultDirNum() {
    int dirNum = 0;
    for (int i = 0; i < TIER_PATH.length; i ++) {
      dirNum += TIER_PATH[i].length;
    }
    return dirNum;
  }
}
