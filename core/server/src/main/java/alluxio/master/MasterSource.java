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
  private boolean mGaugesRegistered = false;
  private final MetricRegistry mMetricRegistry = new MetricRegistry();

  private final Counter mDirectoriesCreated =
          mMetricRegistry.counter(MetricRegistry.name("DirectoriesCreated"));
  private final Counter mFileBlockInfosGot =
          mMetricRegistry.counter(MetricRegistry.name("FileBlockInfosGot"));
  private final Counter mFileInfosGot =
          mMetricRegistry.counter(MetricRegistry.name("FileInfosGot"));
  private final Counter mFilesCompleted =
          mMetricRegistry.counter(MetricRegistry.name("FilesCompleted"));
  private final Counter mFilesCreated =
          mMetricRegistry.counter(MetricRegistry.name("FilesCreated"));
  private final Counter mFilesFreed =
          mMetricRegistry.counter(MetricRegistry.name("FilesFreed"));
  private final Counter mFilesPersisted =
          mMetricRegistry.counter(MetricRegistry.name("FilesPersisted"));
  private final Counter mNewBlocksGot =
          mMetricRegistry.counter(MetricRegistry.name("NewBlocksGot"));
  private final Counter mPathsDeleted =
          mMetricRegistry.counter(MetricRegistry.name("PathsDeleted"));
  private final Counter mPathsMounted =
          mMetricRegistry.counter(MetricRegistry.name("PathsMounted"));
  private final Counter mPathsRenamed =
          mMetricRegistry.counter(MetricRegistry.name("PathsRenamed"));
  private final Counter mPathsUnmounted =
          mMetricRegistry.counter(MetricRegistry.name("PathsUnmounted"));

  private final Counter mCompleteFileOps =
      mMetricRegistry.counter(MetricRegistry.name("CompleteFileOps"));
  private final Counter mCreateDirectoryOps =
          mMetricRegistry.counter(MetricRegistry.name("CreateDirectoryOps"));
  private final Counter mCreateFileOps =
          mMetricRegistry.counter(MetricRegistry.name("CreateFileOps"));
  private final Counter mDeletePathOps =
          mMetricRegistry.counter(MetricRegistry.name("DeletePathOps"));
  private final Counter mFreeFileOps =
      mMetricRegistry.counter(MetricRegistry.name("FreeFileOps"));
  private final Counter mGetFileBlockInfoOps =
          mMetricRegistry.counter(MetricRegistry.name("GetFileBlockInfoOps"));
  private final Counter mGetFileInfoOps =
          mMetricRegistry.counter(MetricRegistry.name("GetFileInfoOps"));
  private final Counter mGetNewBlockOps =
          mMetricRegistry.counter(MetricRegistry.name("GetNewBlockOps"));
  private final Counter mMountOps =
          mMetricRegistry.counter(MetricRegistry.name("MountOps"));
  private final Counter mRenamePathOps =
      mMetricRegistry.counter(MetricRegistry.name("RenamePathOps"));
  private final Counter mSetAttributeOps =
          mMetricRegistry.counter(MetricRegistry.name("SetAttributeOps"));
  private final Counter mUnmountOps =
      mMetricRegistry.counter(MetricRegistry.name("UnmountOps"));

  /**
   * Registers metric gauges.
   *
   * @param alluxioMaster an Alluxio master handle
   */
  public void registerGauges(final AlluxioMaster alluxioMaster) {
    if (mGaugesRegistered) {
      return;
    }
    mMetricRegistry.register(MetricRegistry.name("CapacityTotal"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return alluxioMaster.getBlockMaster().getCapacityBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityUsed"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return alluxioMaster.getBlockMaster().getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityFree"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return alluxioMaster.getBlockMaster().getCapacityBytes()
            - alluxioMaster.getBlockMaster().getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("UnderFsCapacityTotal"), new Gauge<Long>() {
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

    mMetricRegistry.register(MetricRegistry.name("UnderFsCapacityUsed"), new Gauge<Long>() {
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

    mMetricRegistry.register(MetricRegistry.name("UnderFsCapacityFree"), new Gauge<Long>() {
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

    mMetricRegistry.register(MetricRegistry.name("Workers"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return alluxioMaster.getBlockMaster().getWorkerCount();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("PathsTotal"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return alluxioMaster.getFileSystemMaster().getNumberOfPaths();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("FilesPinned"), new Gauge<Integer>() {
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
