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
  private final Counter mFilesCreated =
      mMetricRegistry.counter(MetricRegistry.name("FilesCreated"));
  private final Counter mCreateFileOps =
      mMetricRegistry.counter(MetricRegistry.name("CreateFileOps"));
  private final Counter mFilesDeleted =
      mMetricRegistry.counter(MetricRegistry.name("FilesDeleted"));
  private final Counter mDeleteFileOps =
      mMetricRegistry.counter(MetricRegistry.name("DeleteFileOps"));
  private final Counter mFilesRenamed =
      mMetricRegistry.counter(MetricRegistry.name("FilesRenamed"));
  private final Counter mRenameOps =
      mMetricRegistry.counter(MetricRegistry.name("RenameFileOps"));
  private final Counter mFilesCheckpointed =
      mMetricRegistry.counter(MetricRegistry.name("FilesCheckpointed"));
  private final Counter mGetFileStatusOps =
      mMetricRegistry.counter(MetricRegistry.name("GetFileStatusOps"));

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

    mMetricRegistry.register(MetricRegistry.name("FilesTotal"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return tachyonMaster.getFileSystemMaster().getNumberOfFiles();
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

  public void incCreateFileOps() {
    mCreateFileOps.inc();
  }

  public void incDeleteFileOps() {
    mDeleteFileOps.inc();
  }

  public void incFilesCreated() {
    mFilesCreated.inc();
  }

  public void incFilesCreated(long n) {
    mFilesCreated.inc(n);
  }

  public void incFilesDeleted() {
    mFilesDeleted.inc();
  }

  public void incFilesDeleted(long n) {
    mFilesDeleted.inc(n);
  }

  public void incFilesRenamed() {
    mFilesRenamed.inc();
  }

  public void incFilesCheckpointed() {
    mFilesCheckpointed.inc();
  }

  public void incGetFileStatusOps() {
    mGetFileStatusOps.inc();
  }

  public void incRenameOps() {
    mRenameOps.inc();
  }
}
