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

package tachyon.master;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import tachyon.Constants;
import tachyon.metrics.source.Source;
import tachyon.underfs.UnderFileSystem;

/**
 * A MasterSource collects a Master's internal state. Metrics like *Ops are used to record how many
 * times that operation was attempted so the counter is incremented no matter if it is successful or
 * not.
 */
public class MasterSource implements Source {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String MASTER_SOURCE_NAME = "master";
  private boolean mGaugesRegistered = false;
  private final MetricRegistry mMetricRegistry = new MetricRegistry();

  private final Counter mCompleteFileOps =
      mMetricRegistry.counter(MetricRegistry.name("CompleteFileOps"));
  private final Counter mFilesCompleted =
      mMetricRegistry.counter(MetricRegistry.name("FilesCompleted"));

  private final Counter mFreeFileOps =
      mMetricRegistry.counter(MetricRegistry.name("FreeFileOps"));
  private final Counter mFilesFreed =
      mMetricRegistry.counter(MetricRegistry.name("FilesFreed"));

  private final Counter mFilesCreated =
      mMetricRegistry.counter(MetricRegistry.name("FilesCreated"));
  private final Counter mCreateFileOps =
      mMetricRegistry.counter(MetricRegistry.name("CreateFileOps"));
  private final Counter mDirectoriesCreated =
      mMetricRegistry.counter(MetricRegistry.name("DirectoriesCreated"));
  private final Counter mCreateDirectoryOps =
      mMetricRegistry.counter(MetricRegistry.name("CreateDirectoryOps"));
  private final Counter mPathsDeleted =
      mMetricRegistry.counter(MetricRegistry.name("PathsDeleted"));
  private final Counter mDeletePathOps =
      mMetricRegistry.counter(MetricRegistry.name("DeletePathOps"));
  private final Counter mPathsRenamed =
      mMetricRegistry.counter(MetricRegistry.name("PathsRenamed"));
  private final Counter mRenamePathOps = mMetricRegistry.counter(MetricRegistry.name("RenamePathOps"));
  private final Counter mFilesPersisted =
      mMetricRegistry.counter(MetricRegistry.name("FilesPersisted"));
  private final Counter mPathsMounted =
      mMetricRegistry.counter(MetricRegistry.name("PathsMounted"));
  private final Counter mMountOps =
      mMetricRegistry.counter(MetricRegistry.name("MountOps"));
  private final Counter mPathsUnmounted =
      mMetricRegistry.counter(MetricRegistry.name("PathsUnmounted"));
  private final Counter mUnmountOps =
      mMetricRegistry.counter(MetricRegistry.name("UnmountOps"));
  private final Counter mGetFileInfoOps =
      mMetricRegistry.counter(MetricRegistry.name("GetFileInfoOps"));
  private final Counter mFileInfosGot =
      mMetricRegistry.counter(MetricRegistry.name("FileInfosGot"));
  private final Counter mGetFileBlockInfoOps =
      mMetricRegistry.counter(MetricRegistry.name("GetFileBlockInfoOps"));
  private final Counter mFileBlockInfosGot =
      mMetricRegistry.counter(MetricRegistry.name("FileBlockInfosGot"));
  private final Counter mNewBlockRequestOps =
      mMetricRegistry.counter(MetricRegistry.name("NewBlockRequestOps"));
  private final Counter mNewBlocksRequested =
      mMetricRegistry.counter(MetricRegistry.name("NewBlocksRequested"));

  private final Counter mSetStateOps =
      mMetricRegistry.counter(MetricRegistry.name("SetStateOps"));

  public void registerGauges(final TachyonMaster tachyonMaster) {
    if (mGaugesRegistered) {
      return;
    }
    mMetricRegistry.register(MetricRegistry.name("CapacityTotal"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return tachyonMaster.getBlockMaster().getCapacityBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityUsed"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return tachyonMaster.getBlockMaster().getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityFree"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return tachyonMaster.getBlockMaster().getCapacityBytes()
            - tachyonMaster.getBlockMaster().getUsedBytes();
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
        return tachyonMaster.getBlockMaster().getWorkerCount();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("PathsTotal"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return tachyonMaster.getFileSystemMaster().getNumberOfPaths();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("FilesPinned"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return tachyonMaster.getFileSystemMaster().getNumberOfPinnedFiles();
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

  public void incCompleteFileOps() {
    mCompleteFileOps.inc();
  }

  public void incFilesCompleted() {
    mFilesCompleted.inc();
  }

  public void incFreeFileOps() {
    mFreeFileOps.inc();
  }

  public void incFilesReleased(long n) {
    mFilesFreed.inc(n);
  }

  public void incCreateFileOps(long n) {
    mCreateFileOps.inc(n);
  }

  public void incDeletePathOps(long n) {
    mDeletePathOps.inc(n);
  }

  public void incFilesCreated(long n) {
    mFilesCreated.inc(n);
  }

  public void incDirectoriesCreated(long n) {
    mDirectoriesCreated.inc(n);
  }

  public void incCreateDirectoriesOps(long n) {
    mCreateDirectoryOps.inc();
  }

  public void incPathsDeleted(long n) {
    mPathsDeleted.inc(n);
  }

  public void incPathsRenamed(long n) {
    mPathsRenamed.inc(n);
  }

  public void incFilesPersisted(long n) {
    mFilesPersisted.inc(n);
  }

  public void incGetFileInfoOps(long n) {
    mGetFileInfoOps.inc(n);
  }

  public void incFileInfosGot(long n) {
    mFileInfosGot.inc(n);
  }

  public void incGetFileBlockInfoOps(long n) {
    mGetFileBlockInfoOps.inc(n);
  }

  public void incFileBlockInfosGot(long n) {
    mFileBlockInfosGot.inc(n);
  }

  public void incRenamePathOps(long n) {
    mRenamePathOps.inc(n);
  }

  public void incPathsUnmounted(long n) {
    mPathsUnmounted.inc(n);
  }

  public void incUnmountOps(long n) {
    mUnmountOps.inc(n);
  }

  public void incPathsMounted(long n) {
    mPathsMounted.inc(n);
  }

  public void incMountOps(long n) {
    mMountOps.inc(n);
  }

  public void incSetStateOps(long n) {
    mSetStateOps.inc(n);
  }

  public void incNewBlockRequestOps(long n) {
    mNewBlockRequestOps.inc(n);
  }

  public void incNewBlocksRequested(long n) {
    mNewBlocksRequested.inc(n);
  }
}
