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

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.annotator.BlockIterator;
import alluxio.worker.block.evictor.Evictor;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockWriter;
import alluxio.worker.block.meta.DefaultTempBlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;

/**
 * Utility methods for setting and testing {@link TieredBlockStore}.
 */
public final class TieredBlockStoreTestUtils {
  /**
   * Default configurations of a TieredBlockStore for use in {@link #defaultMetadataManager}. They
   * represent a block store with a MEM tier and a SSD tier, there are two directories with capacity
   * 2000 bytes and 3000 bytes separately in the MEM tier and three directories with capacity 10000,
   * 20000, 30000 bytes separately in the SSD tier.
   */
  public static final int[] TIER_ORDINAL = {0, 1};
  public static final String[] TIER_ALIAS = {Constants.MEDIUM_MEM, Constants.MEDIUM_SSD};
  public static final String[][] TIER_PATH = {{"/mem/0", "/mem/1"}, {"/ssd/0", "/ssd/1", "/ssd/2"}};
  public static final long[][] TIER_CAPACITY_BYTES = {{2000, 3000}, {10000, 20000, 30000}};
  public static final String[][] TIER_MEDIA_TYPES =
      {{Constants.MEDIUM_MEM, Constants.MEDIUM_MEM},
          {Constants.MEDIUM_SSD, Constants.MEDIUM_SSD, Constants.MEDIUM_SSD}};
  public static final String WORKER_DATA_FOLDER = "/alluxioworker/";

  /**
   * Sets up a {@link ServerConfiguration} for a {@link TieredBlockStore} with several tiers
   * configured by the parameters. For simplicity, you can use {@link #setupDefaultConf(String)}
   * which calls this method with default values.
   *
   * @param baseDir the directory path as prefix for all the paths of directories in the tiered
   *        storage; when specified, the directory needs to exist before calling this method
   * @param tierOrdinal like {@link #TIER_ORDINAL}, length must be &gt; 0
   * @param tierAlias like {@link #TIER_ALIAS}, each corresponds to an element in tierLevel
   * @param tierPath like {@link #TIER_PATH}, each list represents directories of the tier with the
   *        same list index in tierAlias
   * @param tierCapacity like {@link #TIER_CAPACITY_BYTES}, should be in the same dimension with
   *        tierPath, each element is the capacity of the corresponding dir in tierPath
   * @param tierMediumType should be in the same dimension with tierPath, each element is the
   *                       medium type of the corresponding dir in tierPath
   * @param workerDataFolder when specified it sets up the alluxio.worker.data.folder property
   */
  public static void setupConfWithMultiTier(String baseDir, int[] tierOrdinal, String[] tierAlias,
      String[][] tierPath, long[][] tierCapacity, String[][] tierMediumType,
      String workerDataFolder) throws Exception {
    // make sure dimensions are legal
    Preconditions.checkNotNull(tierOrdinal, "tierOrdinal");
    Preconditions.checkNotNull(tierAlias, "tierAlias");
    Preconditions.checkNotNull(tierPath, "tierPath");
    Preconditions.checkNotNull(tierCapacity, "tierCapacity");
    Preconditions.checkNotNull(tierMediumType, "tierMediumType");

    Preconditions.checkArgument(tierOrdinal.length > 0, "length of tierLevel should be > 0");
    Preconditions.checkArgument(tierOrdinal.length == tierAlias.length,
        "tierAlias and tierLevel should have the same length");
    Preconditions.checkArgument(tierOrdinal.length == tierPath.length,
        "tierPath and tierLevel should have the same length");
    Preconditions.checkArgument(tierOrdinal.length == tierCapacity.length,
        "tierCapacity and tierLevel should have the same length");
    Preconditions.checkArgument(tierOrdinal.length == tierMediumType.length,
        "tierMediumType and tierLevel should have the same length");
    int nTier = tierOrdinal.length;

    tierPath = createDirHierarchy(baseDir, tierPath);
    if (workerDataFolder != null) {
      ServerConfiguration.set(PropertyKey.WORKER_DATA_FOLDER, workerDataFolder);
    }
    ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_LEVELS, String.valueOf(nTier));

    // sets up each tier in turn
    for (int i = 0; i < nTier; i++) {
      setupConfTier(tierOrdinal[i], tierAlias[i], tierPath[i], tierCapacity[i], tierMediumType[i]);
    }
  }

  /**
   * Sets up a {@link ServerConfiguration} for a {@link TieredBlockStore} with only *one tier*
   * configured by the parameters. For simplicity, you can use {@link #setupDefaultConf(String)}
   * which sets up the tierBlockStore with default values.
   *
   * This method modifies the configuration, so be sure to reset it when done.
   *
   * @param baseDir the directory path as prefix for all the paths of directories in the tiered
   *        storage; when specified, the directory needs to exist before calling this method
   * @param tierOrdinal ordinal of this tier
   * @param tierAlias alias of this tier
   * @param tierPath path of this tier; when `baseDir` is specified, the actual test tierPath turns
   *        into `baseDir/tierPath`
   * @param tierCapacity capacity of this tier
   * @param workerDataFolder when specified it sets up the alluxio.worker.data.folder property
   */
  public static void setupConfWithSingleTier(String baseDir, int tierOrdinal, String tierAlias,
      String[] tierPath, long[] tierCapacity, String[] tierMedia, String workerDataFolder)
      throws Exception {
    if (baseDir != null) {
      tierPath = createDirHierarchy(baseDir, tierPath);
    }
    if (workerDataFolder != null) {
      ServerConfiguration.set(PropertyKey.WORKER_DATA_FOLDER, workerDataFolder);
    }
    ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_LEVELS, String.valueOf(1));
    setupConfTier(tierOrdinal, tierAlias, tierPath, tierCapacity, tierMedia);
  }

  /**
   * Sets up a specific tier's {@link ServerConfiguration} for a {@link TieredBlockStore}.
   *
   * @param tierAlias alias of the tier
   * @param tierPath absolute path of the tier
   * @param tierCapacity capacity of the tier
   */
  private static void setupConfTier(int ordinal, String tierAlias, String[] tierPath,
      long[] tierCapacity, String[] tierMediumType) {
    Preconditions.checkNotNull(tierPath, "tierPath");
    Preconditions.checkNotNull(tierCapacity, "tierCapacity");
    Preconditions.checkNotNull(tierMediumType, "tierMediumType");
    Preconditions.checkArgument(tierPath.length == tierCapacity.length,
        "tierPath and tierCapacity should have the same length");

    ServerConfiguration
        .set(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(ordinal),
            tierAlias);

    String tierPathString = StringUtils.join(tierPath, ",");
    ServerConfiguration
        .set(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(ordinal),
            tierPathString);

    String tierCapacityString = StringUtils.join(ArrayUtils.toObject(tierCapacity), ",");
    ServerConfiguration
        .set(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(ordinal),
            tierCapacityString);

    String tierMediumString = StringUtils.join(tierMediumType, ",");
    ServerConfiguration
        .set(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE.format(ordinal),
            tierMediumString);
  }

  /**
   * Joins baseDir with all the paths listed in the array and then create the new generated path.
   *
   * @param baseDir the directory path as prefix for all the paths in the array 'dirs'
   * @param dirs 2-D array of directory paths
   * @return new joined and created paths array
   */
  private static String[][] createDirHierarchy(String baseDir, final String[][] dirs)
      throws Exception {
    if (baseDir == null) {
      return dirs;
    }
    String[][] newDirs = new String[dirs.length][];
    for (int i = 0; i < dirs.length; i++) {
      newDirs[i] = createDirHierarchy(baseDir, dirs[i]);
    }
    return newDirs;
  }

  /**
   * Joins baseDir with all the paths listed in the array and then create the new generated path.
   *
   * @param baseDir the directory path as prefix for all the paths in the array 'dirs'
   * @param dirs 1-D array of directory paths
   * @return new joined and created paths array
   */
  private static String[] createDirHierarchy(String baseDir, final String[] dirs) throws Exception {
    if (baseDir == null) {
      return dirs;
    }
    String[] newDirs = new String[dirs.length];
    for (int i = 0; i < dirs.length; i++) {
      newDirs[i] = PathUtils.concatPath(baseDir, dirs[i]);
      FileUtils.createDir(newDirs[i]);
    }
    return newDirs;
  }

  /**
   * Creates a BlockMetadataManager with {@link #setupDefaultConf(String)}.
   *
   * @param baseDir the directory path as prefix for paths of directories in the tiered storage; the
   *        directory needs to exist before calling this method
   * @return the created metadata manager
   */
  public static BlockMetadataManager defaultMetadataManager(String baseDir) throws Exception {
    setupDefaultConf(baseDir);
    return BlockMetadataManager.createBlockMetadataManager();
  }

  /**
   * Creates a {@link BlockMetadataEvictorView} with {@link #setupDefaultConf(String)}.
   *
   * @param baseDir the directory path as prefix for paths of directories in the tiered storage; the
   *        directory needs to exist before calling this method
   * @return the created metadata evictor view
   */
  public static BlockMetadataEvictorView defaultMetadataManagerView(String baseDir)
      throws Exception {
    BlockMetadataManager metaManager = TieredBlockStoreTestUtils.defaultMetadataManager(baseDir);
    return new BlockMetadataEvictorView(metaManager, Collections.<Long>emptySet(),
        Collections.<Long>emptySet());
  }

  /**
   * Sets up a {@link ServerConfiguration} with default values of {@link #TIER_ORDINAL},
   * {@link #TIER_ALIAS}, {@link #TIER_PATH} with the baseDir as path prefix,
   * {@link #TIER_CAPACITY_BYTES}.
   *
   * @param baseDir the directory path as prefix for paths of directories in the tiered storage; the
   *        directory needs to exist before calling this method
   */
  public static void setupDefaultConf(String baseDir) throws Exception {
    setupConfWithMultiTier(baseDir, TIER_ORDINAL, TIER_ALIAS, TIER_PATH, TIER_CAPACITY_BYTES,
        TIER_MEDIA_TYPES, WORKER_DATA_FOLDER);
  }

  /**
   * Caches bytes into {@link StorageDir}.
   *
   * @param sessionId session who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param dir the {@link StorageDir} the block resides in
   * @param meta the metadata manager to update meta of the block
   * @param evictor the evictor to be informed of the new block
   */
  public static void cache(long sessionId, long blockId, long bytes, StorageDir dir,
      BlockMetadataManager meta, Evictor evictor) throws Exception {
    BlockStoreEventListener listener = null;
    if (evictor instanceof BlockStoreEventListener) {
      listener = (BlockStoreEventListener) evictor;
    }
    cache2(sessionId, blockId, bytes, dir, meta, listener);
  }

  /**
   * Caches bytes into {@link BlockStore} at specific location.
   *
   * @param sessionId session who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param blockStore block store that the block is written into
   * @param location the location where the block resides
   * @param pinOnCreate whether to pin block on create
   */
  public static void cache(long sessionId, long blockId, long bytes, BlockStore blockStore,
      BlockStoreLocation location, boolean pinOnCreate) throws Exception {
    TempBlockMeta tempBlockMeta = blockStore.createBlock(sessionId, blockId,
        AllocateOptions.forCreate(bytes, location));
    // write data
    BlockWriter writer = new LocalFileBlockWriter(tempBlockMeta.getPath());
    writer.append(BufferUtils.getIncreasingByteBuffer(Ints.checkedCast(bytes)));
    writer.close();

    // commit block
    blockStore.commitBlock(sessionId, blockId, pinOnCreate);
  }

  /**
   * Caches bytes into {@link BlockStore} at specific location.
   *
   * @param sessionId session who caches the data
   * @param blockId id of the cached block
   * @param options allocation options
   * @param blockStore block store that the block is written into
   * @param pinOnCreate whether to pin block on create
   */
  public static void cache(long sessionId, long blockId, AllocateOptions options,
      BlockStore blockStore, boolean pinOnCreate) throws Exception {
    TempBlockMeta tempBlockMeta = blockStore.createBlock(sessionId, blockId, options);
    // write data
    BlockWriter writer = new LocalFileBlockWriter(tempBlockMeta.getPath());
    writer.append(BufferUtils.getIncreasingByteBuffer(Ints.checkedCast(options.getSize())));
    writer.close();

    // commit block
    blockStore.commitBlock(sessionId, blockId, pinOnCreate);
  }

  /**
   * Caches bytes into {@link StorageDir}.
   *
   * @param sessionId session who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param tierLevel tier level of the {@link StorageDir} the block resides in
   * @param dirIndex index of directory in the tierLevel the block resides in
   * @param meta the metadata manager to update meta of the block
   * @param evictor the evictor to be informed of the new block
   */
  public static void cache(long sessionId, long blockId, long bytes, int tierLevel, int dirIndex,
      BlockMetadataManager meta, Evictor evictor) throws Exception {
    StorageDir dir = meta.getTiers().get(tierLevel).getDir(dirIndex);
    cache(sessionId, blockId, bytes, dir, meta, evictor);
  }

  /**
   * Caches bytes into {@link StorageDir}.
   *
   * @param sessionId session who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param dir the {@link StorageDir} the block resides in
   * @param meta the metadata manager to update meta of the block
   * @param iterator the iterator to be informed of the new block
   */
  public static void cache2(long sessionId, long blockId, long bytes, StorageDir dir,
      BlockMetadataManager meta, BlockIterator iterator) throws Exception {
    cache2(sessionId, blockId, bytes, dir, meta, (BlockStoreEventListener) null);
    if (iterator != null) {
      for (BlockStoreEventListener listener : iterator.getListeners()) {
        listener.onCommitBlock(sessionId, blockId, dir.toBlockStoreLocation());
      }
    }
  }

  /**
   * Caches bytes into {@link StorageDir}.
   *
   * @param sessionId session who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param dir the {@link StorageDir} the block resides in
   * @param meta the metadata manager to update meta of the block
   * @param listener the listener to be informed of the new block
   */
  public static void cache2(long sessionId, long blockId, long bytes, StorageDir dir,
      BlockMetadataManager meta, BlockStoreEventListener listener) throws Exception {
    TempBlockMeta tempBlockMeta = createTempBlock(sessionId, blockId, bytes, dir);

    // commit block.
    FileUtils.move(tempBlockMeta.getPath(), tempBlockMeta.getCommitPath());
    meta.commitTempBlockMeta(tempBlockMeta);

    // update iterator if a listener.
    if (listener != null) {
      listener.onCommitBlock(sessionId, blockId, dir.toBlockStoreLocation());
    }
  }

  /**
   * Makes a temp block of a given size in {@link StorageDir}.
   *
   * @param sessionId session who caches the data
   * @param blockId id of the cached block
   * @param bytes size of the block in bytes
   * @param dir the {@link StorageDir} the block resides in
   * @return the temp block meta
   */
  public static TempBlockMeta createTempBlock(long sessionId, long blockId, long bytes,
      StorageDir dir) throws Exception {
    // prepare temp block
    TempBlockMeta tempBlockMeta = new DefaultTempBlockMeta(sessionId, blockId, bytes, dir);
    dir.addTempBlockMeta(tempBlockMeta);

    // write data
    FileUtils.createFile(tempBlockMeta.getPath());
    BlockWriter writer = new LocalFileBlockWriter(tempBlockMeta.getPath());
    writer.append(BufferUtils.getIncreasingByteBuffer(Ints.checkedCast(bytes)));
    writer.close();
    return tempBlockMeta;
  }

  /**
   * Gets the total capacity of all tiers in bytes.
   *
   * @return total capacity of all tiers in bytes
   */
  public static long getDefaultTotalCapacityBytes() {
    long totalCapacity = 0;
    for (long[] tierCapacityBytes : TIER_CAPACITY_BYTES) {
      for (long tierCapacityByte : tierCapacityBytes) {
        totalCapacity += tierCapacityByte;
      }
    }
    return totalCapacity;
  }

  /**
   * Gets the number of testing directories of all tiers.
   *
   * @return number of testing directories of all tiers
   */
  public static long getDefaultDirNum() {
    int dirNum = 0;
    for (String[] tierPath : TIER_PATH) {
      dirNum += tierPath.length;
    }
    return dirNum;
  }
}
