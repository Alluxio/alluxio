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

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

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
 * This class provides utility methods for setting and testing tiered block store.
 */
public class TieredBlockStoreTestUtils {
  /**
   * Default configurations of a TieredBlockStore for use in {@link #defaultMetadataManager}. They
   * represent a block store with a MEM tier and a SSD tier, there are two directories with capacity
   * 2000 bytes and 3000 bytes separately in the MEM tier and three directories with capacity 10000,
   * 20000, 30000 bytes separately in the SSD tier.
   */
  public static final int[] TIER_LEVEL = {0, 1};
  public static final StorageLevelAlias[] TIER_ALIAS = {StorageLevelAlias.MEM,
      StorageLevelAlias.SSD};
  public static final String[][] TIER_PATH =
      {{"/mem/0", "/mem/1"}, {"/ssd/0", "/ssd/1", "/ssd/2"}};
  public static final long[][] TIER_CAPACITY_BYTES = {{2000, 3000}, {10000, 20000, 30000}};
  public static final String WORKER_DATA_FOLDER = "/tachyonworker/";

  public static TachyonConf sTachyonConf = WorkerContext.getConf();

  /**
   * Sets up a {@link TachyonConf} for a {@link TieredBlockStore} with several tiers configured
   * by the parameters. For simplicity, you can use {@link #setupTachyonConfDefault(String)} which
   * calls this method with default values.
   *
   * @param baseDir The directory path as prefix for all the paths of directories in the tiered
   *        storage. When specified, the directory needs to exist before calling this method.
   * @param tierLevel Like {@link #TIER_LEVEL}, length must be &gt; 0.
   * @param tierAlias Like {@link #TIER_ALIAS}, each corresponds to an element in tierLevel.
   * @param tierPath Like {@link #TIER_PATH}, each list represents directories of the tier with the
   *        same list index in tierAlias.
   * @param tierCapacity Like {@link #TIER_CAPACITY_BYTES}, should be in the same dimension with
   *        tierPath, each element is the capacity of the corresponding dir in tierPath.
   * @param workerDataFolder When specified it sets up the tachyon.worker.data.folder property.
   * @throws Exception When error happens during creating temporary folder.
   */
  public static void setupTachyonConfWithMultiTier(String baseDir, int[] tierLevel,
      StorageLevelAlias[] tierAlias, String[][] tierPath, long[][] tierCapacity,
      String workerDataFolder) throws Exception {
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

    tierPath = createDirHierarchy(baseDir, tierPath);
    if (workerDataFolder != null) {
      sTachyonConf.set(Constants.WORKER_DATA_FOLDER, workerDataFolder);
    }
    sTachyonConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, String.valueOf(nTier));

    // sets up each tier in turn
    for (int i = 0; i < nTier; i ++) {
      setupTachyonConfTier(tierLevel[i], tierAlias[i], tierPath[i], tierCapacity[i]);
    }
  }

  /**
   * Sets up a {@link TachyonConf} for a {@link TieredBlockStore} with only *one tier* configured
   * by the parameters. For simplicity, you can use {@link #setupTachyonConfDefault(String)} which
   * sets up the tierBlockStore with default values.
   *
   * @param baseDir The directory path as prefix for all the paths of directories in the tiered
   *        storage. When specified, the directory needs to exist before calling this method.
   * @param tierLevel Level of this tier.
   * @param tierAlias Alias of this tier.
   * @param tierPath Path of this tier. When `baseDir` is specified, the actual test tierPath
   *        turns into `baseDir/tierPath`.
   * @param tierCapacity Capacity of this tier.
   * @param workerDataFolder When specified it sets up the tachyon.worker.data.folder property.
   * @return The created TachyonConf.
   * @throws Exception When error happens during creating temporary folder.
   */
  public static void setupTachyonConfWithSingleTier(String baseDir, int tierLevel,
      StorageLevelAlias tierAlias, String[] tierPath, long[] tierCapacity, String
      workerDataFolder) throws Exception {
    if (baseDir != null) {
      tierPath = createDirHierarchy(baseDir, tierPath);
    }
    if (workerDataFolder != null) {
      sTachyonConf.set(Constants.WORKER_DATA_FOLDER, workerDataFolder);
    }
    sTachyonConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, String.valueOf(1));
    setupTachyonConfTier(tierLevel, tierAlias, tierPath, tierCapacity);
  }

  /**
   * Sets up a specific tier's {@link TachyonConf} for a {@link TieredBlockStore}.
   *
   * @param level Level of the tier.
   * @param tierAlias Alias of the tier.
   * @param tierPath Absolute path of the tier.
   * @param tierCapacity Capacity of the tier
   */
  private static void setupTachyonConfTier(int level, StorageLevelAlias tierAlias,
      String[] tierPath, long[] tierCapacity) {
    Preconditions.checkNotNull(tierPath);
    Preconditions.checkNotNull(tierCapacity);
    Preconditions.checkArgument(tierPath.length == tierCapacity.length,
        String.format("tierPath and tierCapacity should have the same length"));

    sTachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, level),
        tierAlias.toString());

    String tierPathString = StringUtils.join(tierPath, ",");
    sTachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, level),
        tierPathString);

    String tierCapacityString = StringUtils.join(ArrayUtils.toObject(tierCapacity), ",");
    sTachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, level),
        tierCapacityString);
  }

  /**
   * Joins baseDir with all the paths listed in the array and then create the new generated path.
   *
   * @param baseDir The directory path as prefix for all the paths in the array 'dirs'.
   * @param dirs 2-D array of directory paths.
   * @return New joined and created paths array.
   * @throws Exception When error happens during creating temporary folder.
   */
  private static String[][] createDirHierarchy(String baseDir, final String[][] dirs)
      throws Exception {
    if (baseDir == null) {
      return dirs;
    }
    String[][] newDirs = new String[dirs.length][];
    for (int i = 0; i < dirs.length; i ++) {
      newDirs[i] = createDirHierarchy(baseDir, dirs[i]);
    }
    return newDirs;
  }

  /**
   * Joins baseDir with all the paths listed in the array and then create the new generated path.
   *
   * @param baseDir The directory path as prefix for all the paths in the array 'dirs'.
   * @param dirs 1-D array of directory paths.
   * @return New joined and created paths array.
   * @throws IOException When error happens during creating temporary folder.
   */
  private static String[] createDirHierarchy(String baseDir, final String[] dirs)
      throws Exception {
    if (baseDir == null) {
      return dirs;
    }
    String[] newDirs = new String[dirs.length];
    for (int i = 0; i < dirs.length; i ++) {
      newDirs[i] = PathUtils.concatPath(baseDir, dirs[i]);
      FileUtils.createDir(newDirs[i]);
    }
    return newDirs;
  }

  /**
   * Creates a BlockMetadataManager with {@link #setupTachyonConfDefault}.
   *
   * @param baseDir The directory path as prefix for paths of directories in the tiered storage. The
   *        directory needs to exist before calling this method.
   * @return The created metadata manager.
   * @throws Exception When error happens during creating temporary folder.
   */
  public static BlockMetadataManager defaultMetadataManager(String baseDir) throws Exception {
    setupTachyonConfDefault(baseDir);
    return BlockMetadataManager.newBlockMetadataManager();
  }

  /**
   * Creates a BlockMetadataManagerView with {@link #setupTachyonConfDefault}.
   *
   * @param baseDir The directory path as prefix for paths of directories in the tiered storage. The
   *        directory needs to exist before calling this method.
   * @return The created metadata manager view.
   * @throws Exception When error happens during creating temporary folder.
   */
  public static BlockMetadataManagerView defaultMetadataManagerView(String baseDir)
      throws Exception {
    BlockMetadataManager metaManager = TieredBlockStoreTestUtils.defaultMetadataManager(baseDir);
    return new BlockMetadataManagerView(metaManager, Collections.<Long>emptySet(),
        Collections.<Long>emptySet());
  }

  /**
   * Sets up a {@link TachyonConf} with default values of {@link #TIER_LEVEL}, {@link #TIER_ALIAS},
   * {@link #TIER_PATH} with the baseDir as path prefix, {@link #TIER_CAPACITY_BYTES}.
   *
   * @param baseDir The directory path as prefix for paths of directories in the tiered storage. The
   *        directory needs to exist before calling this method.
   * @throws Exception When error happens during creating temporary folder.
   */
  public static void setupTachyonConfDefault(String baseDir) throws Exception {
    setupTachyonConfWithMultiTier(baseDir, TIER_LEVEL, TIER_ALIAS, TIER_PATH, TIER_CAPACITY_BYTES,
        WORKER_DATA_FOLDER);
  }

  /**
   * Caches bytes into StorageDir.
   *
   * @param sessionId session who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param dir the StorageDir the block resides in
   * @param meta the metadata manager to update meta of the block
   * @param evictor the evictor to be informed of the new block
   * @throws Exception when fail to cache
   */
  public static void cache(long sessionId, long blockId, long bytes, StorageDir dir,
      BlockMetadataManager meta, Evictor evictor) throws Exception {
    TempBlockMeta tempBlockMeta = createTempBlock(sessionId, blockId, bytes, dir);

    // commit block
    FileUtils.move(tempBlockMeta.getPath(), tempBlockMeta.getCommitPath());
    meta.commitTempBlockMeta(tempBlockMeta);

    // update evictor
    if (evictor instanceof BlockStoreEventListener) {
      ((BlockStoreEventListener) evictor)
          .onCommitBlock(sessionId, blockId, dir.toBlockStoreLocation());
    }
  }

  /**
   * Caches bytes into StorageDir.
   *
   * @param userId user who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param tierLevel tier level of the StorageDir the block resides in
   * @param dirIndex index of directory in the tierLevel the block resides in
   * @param meta the metadata manager to update meta of the block
   * @param evictor the evictor to be informed of the new block
   * @throws Exception when fail to cache
   */
  public static void cache(long sessionId, long blockId, long bytes, int tierLevel, int dirIndex,
      BlockMetadataManager meta, Evictor evictor) throws Exception {
    StorageDir dir = meta.getTiers().get(tierLevel).getDir(dirIndex);
    cache(sessionId, blockId, bytes, dir, meta, evictor);
  }

  /**
   * Makes a temp block of a given size in StorageDir.
   *
   * @param sessionId session who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param dir the StorageDir the block resides in
   * @return the temp block meta
   * @throws Exception when fail to create this block
   */
  public static TempBlockMeta createTempBlock(long sessionId, long blockId, long bytes,
      StorageDir dir) throws Exception {
    // prepare temp block
    TempBlockMeta tempBlockMeta = new TempBlockMeta(sessionId, blockId, bytes, dir);
    dir.addTempBlockMeta(tempBlockMeta);

    // write data
    FileUtils.createFile(tempBlockMeta.getPath());
    BlockWriter writer = new LocalFileBlockWriter(tempBlockMeta);
    writer.append(BufferUtils.getIncreasingByteBuffer(Ints.checkedCast(bytes)));
    writer.close();
    return tempBlockMeta;
  }

  /**
   * Gets the total capacity of all tiers in bytes.
   *
   * @return Total capacity of all tiers in bytes.
   */
  public static long getDefaultTotalCapacityBytes() {
    long totalCapacity = 0;
    for (int i = 0; i < TIER_CAPACITY_BYTES.length; i ++) {
      for (int j = 0; j < TIER_CAPACITY_BYTES[i].length; j ++) {
        totalCapacity += TIER_CAPACITY_BYTES[i][j];
      }
    }
    return totalCapacity;
  }

  /**
   * Gets the number of testing directories of all tiers.
   *
   * @return Number of testing directories of all tiers.
   */
  public static long getDefaultDirNum() {
    int dirNum = 0;
    for (int i = 0; i < TIER_PATH.length; i ++) {
      dirNum += TIER_PATH[i].length;
    }
    return dirNum;
  }
}
