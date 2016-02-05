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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.curator.test.TestingServer;

import com.google.common.base.Throwables;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.util.ClientTestUtils;
import alluxio.Configuration;
import alluxio.exception.ConnectionFailedException;
import alluxio.underfs.UnderFileSystem;
import alluxio.worker.WorkerContext;

/**
 * A local Alluxio cluster with multiple masters.
 */
@NotThreadSafe
public class LocalAlluxioClusterMultiMaster extends AbstractLocalAlluxioCluster {

  private TestingServer mCuratorServer = null;
  private int mNumOfMasters = 0;

  private final List<LocalAlluxioMaster> mMasters = new ArrayList<LocalAlluxioMaster>();

  /**
   * @param workerCapacityBytes the capacity of the worker in bytes
   * @param masters the number of the master
   * @param userBlockSize the block size for a user
   */
  public LocalAlluxioClusterMultiMaster(long workerCapacityBytes, int masters, int userBlockSize) {
    super(workerCapacityBytes, userBlockSize);
    mNumOfMasters = masters;

    try {
      mCuratorServer = new TestingServer();
      LOG.info("Started testing zookeeper: {}", mCuratorServer.getConnectString());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public synchronized FileSystem getClient() throws IOException {
    return getMaster().getClient();
  }

  /**
   * @return the URI of the master
   */
  public String getUri() {
    return new StringBuilder()
        .append(Constants.HEADER_FT)
        .append(mHostname)
        .append(":")
        .append(getMaster().getRPCLocalPort())
        .toString();
  }

  @Override
  public LocalAlluxioMaster getMaster() {
    for (LocalAlluxioMaster master : mMasters) {
      // Return the leader master, if possible.
      if (master.isServing()) {
        return master;
      }
    }
    return mMasters.get(0);
  }

  /**
   * @return index of leader master in {@link #mMasters}, or -1 if there is no leader temporarily
   */
  public int getLeaderIndex() {
    for (int i = 0; i < mNumOfMasters; i ++) {
      if (mMasters.get(i).isServing()) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Iterates over the masters in the order of master creation, kill the first standby master.
   *
   * @return true if a standby master is successfully killed, otherwise, false
   */
  public boolean killStandby() {
    for (int k = 0; k < mNumOfMasters; k ++) {
      if (!mMasters.get(k).isServing()) {
        try {
          LOG.info("master {} is a standby. killing it...", k);
          mMasters.get(k).kill();
          LOG.info("master {} killed.", k);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          return false;
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Iterates over the masters in the order of master creation, kill the leader master.
   *
   * @return true if the leader master is successfully killed, false otherwise
   */
  public boolean killLeader() {
    for (int k = 0; k < mNumOfMasters; k ++) {
      if (mMasters.get(k).isServing()) {
        try {
          LOG.info("master {} is the leader. killing it...", k);
          mMasters.get(k).kill();
          LOG.info("master {} killed.", k);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          return false;
        }
        return true;
      }
    }
    return false;
  }

  private void deleteDir(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, mMasterConf);

    if (ufs.exists(path) && !ufs.delete(path, true)) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  private void mkdir(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, mMasterConf);

    if (ufs.exists(path)) {
      ufs.delete(path, true);
    }
    if (!ufs.mkdirs(path, true)) {
      throw new IOException("Failed to make folder: " + path);
    }
  }

  @Override
  protected void startWorker(Configuration conf) throws IOException, ConnectionFailedException {
    mWorkerConf = WorkerContext.getConf();
    mWorkerConf.merge(conf);

    mWorkerConf.set(Constants.WORKER_WORKER_BLOCK_THREADS_MAX, "100");

    runWorker();
    // The client context should reflect the updates to the conf.
    ClientContext.getConf().merge(conf);
    ClientTestUtils.reinitializeClientContext();
  }

  @Override
  protected void startMaster(Configuration conf) throws IOException {
    mMasterConf = conf;
    mMasterConf.set(Constants.ZOOKEEPER_ENABLED, "true");
    mMasterConf.set(Constants.ZOOKEEPER_ADDRESS, mCuratorServer.getConnectString());
    mMasterConf.set(Constants.ZOOKEEPER_ELECTION_PATH, "/election");
    mMasterConf.set(Constants.ZOOKEEPER_LEADER_PATH, "/leader");
    MasterContext.reset(mMasterConf);

    for (int k = 0; k < mNumOfMasters; k ++) {
      final LocalAlluxioMaster master = LocalAlluxioMaster.create(mHome);
      master.start();
      LOG.info("master NO.{} started, isServing: {}, address: {}", k, master.isServing(),
          master.getAddress());
      mMasters.add(master);
      // Each master should generate a new port for binding
      mMasterConf.set(Constants.MASTER_RPC_PORT, "0");
    }

    // Create the UFS directory after LocalAlluxioMaster construction, because LocalAlluxioMaster
    // sets UNDERFS_ADDRESS.
    mkdir(mMasterConf.get(Constants.UNDERFS_ADDRESS));

    LOG.info("all {} masters started.", mNumOfMasters);
    LOG.info("waiting for a leader.");
    boolean hasLeader = false;
    while (!hasLeader) {
      for (int i = 0; i < mMasters.size(); i ++) {
        if (mMasters.get(i).isServing()) {
          LOG.info("master NO.{} is selected as leader. address: {}", i,
              mMasters.get(i).getAddress());
          hasLeader = true;
          break;
        }
      }
    }
    // Use first master port
    mMasterConf.set(Constants.MASTER_RPC_PORT, String.valueOf(getMaster().getRPCLocalPort()));
  }

  @Override
  public void stopTFS() throws Exception {
    mWorker.stop();
    for (int k = 0; k < mNumOfMasters; k ++) {
      // Use kill() instead of stop(), because stop() does not work well in multi-master mode.
      mMasters.get(k).kill();
    }
    LOG.info("Stopping testing zookeeper: {}", mCuratorServer.getConnectString());
    mCuratorServer.stop();
  }
}
