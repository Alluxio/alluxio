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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LeaderSelectorClient;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.ReadOnlyJournal;
import alluxio.master.lineage.LineageMaster;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The fault tolerant version of {@link AlluxioMaster} that uses zookeeper and standby masters.
 */
@NotThreadSafe
final class FaultTolerantAlluxioMaster extends AlluxioMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The zookeeper client that handles selecting the leader. */
  private LeaderSelectorClient mLeaderSelectorClient = null;

  public FaultTolerantAlluxioMaster() {
    super();
    Configuration conf = MasterContext.getConf();
    Preconditions.checkArgument(conf.getBoolean(Constants.ZOOKEEPER_ENABLED));

    // Set up zookeeper specific functionality.
    try {
      // InetSocketAddress.toString causes test issues, so build the string by hand
      String zkName = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, conf) + ":"
          + getMasterAddress().getPort();
      String zkAddress = conf.get(Constants.ZOOKEEPER_ADDRESS);
      String zkElectionPath = conf.get(Constants.ZOOKEEPER_ELECTION_PATH);
      String zkLeaderPath = conf.get(Constants.ZOOKEEPER_LEADER_PATH);
      mLeaderSelectorClient =
          new LeaderSelectorClient(zkAddress, zkElectionPath, zkLeaderPath, zkName);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void start() throws Exception {
    try {
      mLeaderSelectorClient.start();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }

    Thread currentThread = Thread.currentThread();
    mLeaderSelectorClient.setCurrentMasterThread(currentThread);
    boolean started = false;

    while (true) {
      if (mLeaderSelectorClient.isLeader()) {
        stopServing();
        stopMasters();

        // Transitioning from standby to master, replace readonly journal with writable journal.
        mBlockMaster.upgradeToReadWriteJournal(mBlockMasterJournal);
        mFileSystemMaster.upgradeToReadWriteJournal(mFileSystemMasterJournal);
        mLineageMaster.upgradeToReadWriteJournal(mLineageMasterJournal);

        startMasters(true);
        started = true;
        startServing("(gained leadership)", "(lost leadership)");
      } else {
        // This master should be standby, and not the leader
        if (isServing() || !started) {
          // Need to transition this master to standby mode.
          stopServing();
          stopMasters();

          // When transitioning from master to standby, recreate the masters with a readonly
          // journal.
          mBlockMaster = new BlockMaster(new ReadOnlyJournal(mBlockMasterJournal.getDirectory()));
          mFileSystemMaster = new FileSystemMaster(mBlockMaster,
              new ReadOnlyJournal(mFileSystemMasterJournal.getDirectory()));
          mLineageMaster = new LineageMaster(mFileSystemMaster,
              new ReadOnlyJournal(mLineageMasterJournal.getDirectory()));
          startMasters(false);
          started = true;
        }
        // This master is already in standby mode. No further actions needed.
      }

      CommonUtils.sleepMs(LOG, 100);
    }
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    if (mLeaderSelectorClient != null) {
      mLeaderSelectorClient.close();
    }
  }
}
