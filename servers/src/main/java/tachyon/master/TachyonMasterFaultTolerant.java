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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.LeaderSelectorClient;
import tachyon.Version;
import tachyon.conf.TachyonConf;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.rawtable.RawTableMaster;
import tachyon.util.CommonUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;

/**
 * The fault tolerant version of TachyonMaster that uses zookeeper and standby masters.
 */
public final class TachyonMasterFaultTolerant extends TachyonMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private LeaderSelectorClient mLeaderSelectorClient = null;

  public TachyonMasterFaultTolerant(TachyonConf tachyonConf) {
    super(tachyonConf);
    Preconditions.checkArgument(tachyonConf.getBoolean(Constants.USE_ZOOKEEPER));

    // Set up zookeeper specific functionality.
    try {
      // InetSocketAddress.toString causes test issues, so build the string by hand
      String zkName = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, mTachyonConf) + ":"
          + getMasterAddress().getPort();
      String zkAddress = mTachyonConf.get(Constants.ZOOKEEPER_ADDRESS);
      String zkElectionPath = mTachyonConf.get(Constants.ZOOKEEPER_ELECTION_PATH);
      String zkLeaderPath = mTachyonConf.get(Constants.ZOOKEEPER_LEADER_PATH);
      mLeaderSelectorClient =
          new LeaderSelectorClient(zkAddress, zkElectionPath, zkLeaderPath, zkName);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Starts a Tachyon master server.
   */
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
        stopMasters();

        if (started) {
          // Transitioning from standby to master, replace readonly journal with writable journal.
          mBlockMaster = new BlockMaster(mTachyonConf, mBlockMasterJournal);
          mFileSystemMaster = new FileSystemMaster(mTachyonConf, mBlockMaster,
              mFileSystemMasterJournal);
          mRawTableMaster = new RawTableMaster(mTachyonConf, mFileSystemMaster,
              mRawTableMasterJournal);
        }
        startMasters(true);
        started = true;
        startServing();
      } else {
        // This master should be standby, and not the leader
        if (isServing() || !started) {
          // Need to transition this master to standby mode.
          stopServing();
          stopMasters();

          // When transitioning from master to standby, recreate the masters with a readonly
          // journal.
          mBlockMaster = new BlockMaster(mTachyonConf, mBlockMasterJournal.getReadOnlyJournal());
          mFileSystemMaster = new FileSystemMaster(mTachyonConf, mBlockMaster,
              mFileSystemMasterJournal.getReadOnlyJournal());
          mRawTableMaster = new RawTableMaster(mTachyonConf, mFileSystemMaster,
              mRawTableMasterJournal.getReadOnlyJournal());
          startMasters(false);
          started = true;
        }
        // This master is already in standby mode. No further actions needed.
      }

      CommonUtils.sleepMs(LOG, 100);
    }
  }

  /**
   * Stops a Tachyon master server. Should only be called by tests.
   */
  @Override
  public void stop() throws Exception {
    super.stop();
    if (mLeaderSelectorClient != null) {
      mLeaderSelectorClient.close();
    }
  }

  private void startServing() {
    startServingWebServer();
    LOG.info("Tachyon Master version " + Version.VERSION + " started (gained leadership) @ "
        + getMasterAddress());
    startServingRPCServer();
    LOG.info("Tachyon Master version " + Version.VERSION + " ended (lost leadership) @ "
        + getMasterAddress());
  }
}
