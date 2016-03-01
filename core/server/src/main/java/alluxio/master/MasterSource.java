/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.Constants;
import alluxio.metrics.source.Source;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Master source collects information about master's internal state. Metrics like *Ops are used to
 * record how many times that operation was attempted so the counter is incremented no matter if it
 * is successful or not.
 */
@NotThreadSafe
public class MasterSource implements Source {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String MASTER_SOURCE_NAME = "master";

  public static final String DIRECTORIES_CREATED = "DirectoriesCreated";
  public static final String FILE_BLOCK_INFOS_GOT = "FileBlockInfosGot";
  public static final String FILE_INFOS_GOT = "FileInfosGot";
  public static final String FILES_COMPLETED = "FilesCompleted";
  public static final String FILES_CREATED = "FilesCreated";
  public static final String FILES_FREED = "FilesFreed";
  public static final String FILES_PERSISTED = "FilesPersisted";
  public static final String NEW_BLOCKS_GOT = "NewBlocksGot";
  public static final String PATHS_DELETED = "PathsDeleted";
  public static final String PATHS_MOUNTED = "PathsMounted";
  public static final String PATHS_RENAMED = "PathsRenamed";
  public static final String PATHS_UNMOUNTED = "PathsUnmounted";
  public static final String COMPLETE_FILE_OPS = "CompleteFileOps";
  public static final String CREATE_DIRECTORY_OPS = "CreateDirectoryOps";
  public static final String CREATE_FILE_OPS = "CreateFileOps";
  public static final String DELETE_PATH_OPS = "DeletePathOps";
  public static final String FREE_FILE_OPS = "FreeFileOps";
  public static final String GET_FILE_BLOCK_INFO_OPS = "GetFileBlockInfoOps";
  public static final String GET_FILE_INFO_OPS = "GetFileInfoOps";
  public static final String GET_NEW_BLOCK_OPS = "GetNewBlockOps";
  public static final String MOUNT_OPS = "MountOps";
  public static final String RENAME_PATH_OPS = "RenamePathOps";
  public static final String SET_ATTRIBUTE_OPS = "SetAttributeOps";
  public static final String UNMOUNT_OPS = "UnmountOps";
  public static final String CAPACITY_TOTAL = "CapacityTotal";
  public static final String CAPACITY_USED = "CapacityUsed";
  public static final String CAPACITY_FREE = "CapacityFree";
  public static final String UFS_CAPACITY_TOTAL = "UfsCapacityTotal";
  public static final String UFS_CAPACITY_USED = "UfsCapacityUsed";
  public static final String UFS_CAPACITY_FREE = "UfsCapacityFree";
  public static final String WORKERS = "Workers";
  public static final String PATHS_TOTAL = "PathsTotal";
  public static final String FILES_PINNED = "FilesPinned";

  private boolean mGaugesRegistered = false;
  private final MetricRegistry mMetricRegistry = new MetricRegistry();

  private final Counter mDirectoriesCreated =
      mMetricRegistry.counter(MetricRegistry.name(DIRECTORIES_CREATED));
  private final Counter mFileBlockInfosGot =
      mMetricRegistry.counter(MetricRegistry.name(FILE_BLOCK_INFOS_GOT));
  private final Counter mFileInfosGot =
      mMetricRegistry.counter(MetricRegistry.name(FILE_INFOS_GOT));
  private final Counter mFilesCompleted =
      mMetricRegistry.counter(MetricRegistry.name(FILES_COMPLETED));
  private final Counter mFilesCreated =
      mMetricRegistry.counter(MetricRegistry.name(FILES_CREATED));
  private final Counter mFilesFreed =
      mMetricRegistry.counter(MetricRegistry.name(FILES_FREED));
  private final Counter mFilesPersisted =
      mMetricRegistry.counter(MetricRegistry.name(FILES_PERSISTED));
  private final Counter mNewBlocksGot =
      mMetricRegistry.counter(MetricRegistry.name(NEW_BLOCKS_GOT));
  private final Counter mPathsDeleted =
      mMetricRegistry.counter(MetricRegistry.name(PATHS_DELETED));
  private final Counter mPathsMounted =
      mMetricRegistry.counter(MetricRegistry.name(PATHS_MOUNTED));
  private final Counter mPathsRenamed =
      mMetricRegistry.counter(MetricRegistry.name(PATHS_RENAMED));
  private final Counter mPathsUnmounted =
      mMetricRegistry.counter(MetricRegistry.name(PATHS_UNMOUNTED));

  private final Counter mCompleteFileOps =
      mMetricRegistry.counter(MetricRegistry.name(COMPLETE_FILE_OPS));
  private final Counter mCreateDirectoryOps =
      mMetricRegistry.counter(MetricRegistry.name(CREATE_DIRECTORY_OPS));
  private final Counter mCreateFileOps =
      mMetricRegistry.counter(MetricRegistry.name(CREATE_FILE_OPS));
  private final Counter mDeletePathOps =
      mMetricRegistry.counter(MetricRegistry.name(DELETE_PATH_OPS));
  private final Counter mFreeFileOps =
      mMetricRegistry.counter(MetricRegistry.name(FREE_FILE_OPS));
  private final Counter mGetFileBlockInfoOps =
      mMetricRegistry.counter(MetricRegistry.name(GET_FILE_BLOCK_INFO_OPS));
  private final Counter mGetFileInfoOps =
      mMetricRegistry.counter(MetricRegistry.name(GET_FILE_INFO_OPS));
  private final Counter mGetNewBlockOps =
      mMetricRegistry.counter(MetricRegistry.name(GET_NEW_BLOCK_OPS));
  private final Counter mMountOps =
      mMetricRegistry.counter(MetricRegistry.name(MOUNT_OPS));
  private final Counter mRenamePathOps =
      mMetricRegistry.counter(MetricRegistry.name(RENAME_PATH_OPS));
  private final Counter mSetAttributeOps =
      mMetricRegistry.counter(MetricRegistry.name(SET_ATTRIBUTE_OPS));
  private final Counter mUnmountOps =
      mMetricRegistry.counter(MetricRegistry.name(UNMOUNT_OPS));

  /**
   * Registers metric gauges.
   *
   * @param alluxioMaster an Alluxio master handle
   */
  public void registerGauges(final AlluxioMaster alluxioMaster) {
    if (mGaugesRegistered) {
      return;
    }
    mMetricRegistry.register(MetricRegistry.name(CAPACITY_TOTAL), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return alluxioMaster.getBlockMaster().getCapacityBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name(CAPACITY_USED), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return alluxioMaster.getBlockMaster().getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name(CAPACITY_FREE), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return alluxioMaster.getBlockMaster().getCapacityBytes() - alluxioMaster.getBlockMaster()
            .getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name(UFS_CAPACITY_TOTAL), new Gauge<Long>() {
      @Override
      public Long getValue() {
        long ret = 0L;
        try {
          String ufsDataFolder = MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
          UnderFileSystem ufs = UnderFileSystem.get(ufsDataFolder, MasterContext.getConf());
          ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_TOTAL);
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
        return ret;
      }
    });

    mMetricRegistry.register(MetricRegistry.name(UFS_CAPACITY_USED), new Gauge<Long>() {
      @Override
      public Long getValue() {
        long ret = 0L;
        try {
          String ufsDataFolder = MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
          UnderFileSystem ufs = UnderFileSystem.get(ufsDataFolder, MasterContext.getConf());
          ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_USED);
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
        return ret;
      }
    });

    mMetricRegistry.register(MetricRegistry.name(UFS_CAPACITY_FREE), new Gauge<Long>() {
      @Override
      public Long getValue() {
        long ret = 0L;
        try {
          String ufsDataFolder = MasterContext.getConf().get(Constants.UNDERFS_ADDRESS);
          UnderFileSystem ufs = UnderFileSystem.get(ufsDataFolder, MasterContext.getConf());
          ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_FREE);
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
        return ret;
      }
    });

    mMetricRegistry.register(MetricRegistry.name(WORKERS), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return alluxioMaster.getBlockMaster().getWorkerCount();
      }
    });

    mMetricRegistry.register(MetricRegistry.name(PATHS_TOTAL), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return alluxioMaster.getFileSystemMaster().getNumberOfPaths();
      }
    });

    mMetricRegistry.register(MetricRegistry.name(FILES_PINNED), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return alluxioMaster.getFileSystemMaster().getNumberOfPinnedFiles();
      }
    });

    mGaugesRegistered = true;
  }

  @Override
  public String getName() {
    return MASTER_SOURCE_NAME;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return mMetricRegistry;
  }

  /**
   * Increments the counter of created directories.
   *
   * @param n the increment
   */
  public void incDirectoriesCreated(long n) {
    mDirectoriesCreated.inc(n);
  }

  /**
   * Increments the counter of {@link FileBlockInfo}s requests.
   *
   * @param n the increment
   */
  public void incFileBlockInfosGot(long n) {
    mFileBlockInfosGot.inc(n);
  }

  /**
   * Increments the counter of {@link FileInfo}s requests.
   *
   * @param n the increment
   */
  public void incFileInfosGot(long n) {
    mFileInfosGot.inc(n);
  }

  /**
   * Increments the counter of completed files.
   *
   * @param n the increment
   */
  public void incFilesCompleted(long n) {
    mFilesCompleted.inc(n);
  }

  /**
   * Increments the counter of created files.
   *
   * @param n the increment
   */
  public void incFilesCreated(long n) {
    mFilesCreated.inc(n);
  }

  /**
   * Increments the counter of freed files.
   *
   * @param n the increment
   */
  public void incFilesFreed(long n) {
    mFilesFreed.inc(n);
  }

  /**
   * Increments the counter of persisted files.
   *
   * @param n the increment
   */
  public void incFilesPersisted(long n) {
    mFilesPersisted.inc(n);
  }

  /**
   * Increments the counter of new blocks requests.
   *
   * @param n the increment
   */
  public void incNewBlocksGot(long n) {
    mNewBlocksGot.inc(n);
  }

  /**
   * Increments the counter of deleted paths.
   *
   * @param n the increment
   */
  public void incPathsDeleted(long n) {
    mPathsDeleted.inc(n);
  }

  /**
   * Increments the counter of mounted paths.
   *
   * @param n the increment
   */
  public void incPathsMounted(long n) {
    mPathsMounted.inc(n);
  }

  /**
   * Increments the counter of renamed paths.
   *
   * @param n the increment
   */
  public void incPathsRenamed(long n) {
    mPathsRenamed.inc(n);
  }

  /**
   * Increments the counter of unmounted paths.
   *
   * @param n the increment
   */
  public void incPathsUnmounted(long n) {
    mPathsUnmounted.inc(n);
  }

  /**
   * Increments the counter of complete file RPCs.
   *
   * @param n the increment
   */
  public void incCompleteFileOps(long n) {
    mCompleteFileOps.inc(n);
  }

  /**
   * Increments the counter of create directory RPCs.
   *
   * @param n the increment
   */
  public void incCreateDirectoriesOps(long n) {
    mCreateDirectoryOps.inc();
  }

  /**
   * Increments the counter of create file RPCs.
   *
   * @param n the increment
   */
  public void incCreateFileOps(long n) {
    mCreateFileOps.inc(n);
  }

  /**
   * Increments the counter of delete path RPCs.
   *
   * @param n the increment
   */
  public void incDeletePathOps(long n) {
    mDeletePathOps.inc(n);
  }

  /**
   * Increments the counter of free file RPCs.
   *
   * @param n the increment
   */
  public void incFreeFileOps(long n) {
    mFreeFileOps.inc(n);
  }

  /**
   * Increments the counter of get file block info RPCs.
   *
   * @param n the increment
   */
  public void incGetFileBlockInfoOps(long n) {
    mGetFileBlockInfoOps.inc(n);
  }

  /**
   * Increments the counter of get file info RPCs.
   *
   * @param n the increment
   */
  public void incGetFileInfoOps(long n) {
    mGetFileInfoOps.inc(n);
  }

  /**
   * Increments the counter of get new block RPCs.
   *
   * @param n the increment
   */
  public void incGetNewBlockOps(long n) {
    mGetNewBlockOps.inc(n);
  }

  /**
   * Increments the counter of mount RPCs.
   *
   * @param n the increment
   */
  public void incMountOps(long n) {
    mMountOps.inc(n);
  }

  /**
   * Increments the counter of rename path RPCs.
   *
   * @param n the increment
   */
  public void incRenamePathOps(long n) {
    mRenamePathOps.inc(n);
  }

  /**
   * Increments the counter of set attribute RPCs.
   *
   * @param n the increment
   */
  public void incSetAttributeOps(long n) {
    mSetAttributeOps.inc(n);
  }

  /**
   * Increments the counter of unmount RPCs.
   *
   * @param n the increment
   */
  public void incUnmountOps(long n) {
    mUnmountOps.inc(n);
  }
}
