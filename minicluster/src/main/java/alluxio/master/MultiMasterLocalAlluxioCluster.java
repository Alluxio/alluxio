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

package alluxio.master;

import alluxio.AlluxioTestDirectory;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.exception.ConnectionFailedException;
import alluxio.underfs.UnderFileSystem;
import alluxio.worker.AlluxioWorkerService;

import com.google.common.base.Throwables;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local Alluxio cluster with multiple masters.
 */
@NotThreadSafe
public final class MultiMasterLocalAlluxioCluster extends AbstractLocalAlluxioCluster {

  private TestingServer mCuratorServer = null;
  private int mNumOfMasters = 0;

  private final List<LocalAlluxioMaster> mMasters = new ArrayList<>();

  /**
   * Runs a multi master local Alluxio cluster with a single worker.
   *
   * @param masters the number masters to run
   */
  public MultiMasterLocalAlluxioCluster(int masters) {
    this(masters, 1);
  }

  /**
   * @param masters the number of masters to run
   * @param numWorkers the number of workers to run
   */
  public MultiMasterLocalAlluxioCluster(int masters, int numWorkers) {
    super(numWorkers);
    mNumOfMasters = masters;

    try {
      mCuratorServer = new TestingServer(-1, AlluxioTestDirectory.createTemporaryDirectory("zk"));
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
    return Constants.HEADER_FT + mHostname + ":" + getMaster().getRPCLocalPort();
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
    for (int i = 0; i < mNumOfMasters; i++) {
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
    for (int k = 0; k < mNumOfMasters; k++) {
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
    for (int k = 0; k < mNumOfMasters; k++) {
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
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (ufs.exists(path) && !ufs.delete(path, true)) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  private void mkdir(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (ufs.exists(path)) {
      ufs.delete(path, true);
    }
    if (!ufs.mkdirs(path, true)) {
      throw new IOException("Failed to make folder: " + path);
    }
  }

  @Override
  protected void startWorkers() throws IOException, ConnectionFailedException {
    Configuration.set(PropertyKey.WORKER_BLOCK_THREADS_MAX, "100");
    runWorkers();
  }

  @Override
  protected void startMaster() throws IOException {
    Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, "true");
    Configuration.set(PropertyKey.ZOOKEEPER_ADDRESS, mCuratorServer.getConnectString());
    Configuration.set(PropertyKey.ZOOKEEPER_ELECTION_PATH, "/election");
    Configuration.set(PropertyKey.ZOOKEEPER_LEADER_PATH, "/leader");

    for (int k = 0; k < mNumOfMasters; k++) {
      final LocalAlluxioMaster master = LocalAlluxioMaster.create(mWorkDirectory);
      master.start();
      LOG.info("master NO.{} started, isServing: {}, address: {}", k, master.isServing(),
          master.getAddress());
      mMasters.add(master);
      // Each master should generate a new port for binding
      Configuration.set(PropertyKey.MASTER_RPC_PORT, "0");
    }

    // Create the UFS directory after LocalAlluxioMaster construction, because LocalAlluxioMaster
    // sets UNDERFS_ADDRESS.
    mkdir(Configuration.get(PropertyKey.UNDERFS_ADDRESS));

    LOG.info("all {} masters started.", mNumOfMasters);
    LOG.info("waiting for a leader.");
    boolean hasLeader = false;
    while (!hasLeader) {
      for (int i = 0; i < mMasters.size(); i++) {
        if (mMasters.get(i).isServing()) {
          LOG.info("master NO.{} is selected as leader. address: {}", i,
              mMasters.get(i).getAddress());
          hasLeader = true;
          break;
        }
      }
    }
    // Use first master port
    Configuration.set(PropertyKey.MASTER_RPC_PORT, String.valueOf(getMaster().getRPCLocalPort()));
  }

  @Override
  public void stopFS() throws Exception {
    for (AlluxioWorkerService worker : mWorkers) {
      worker.stop();
    }
    for (int k = 0; k < mNumOfMasters; k++) {
      // TODO(jiri): use stop() instead of kill() (see ALLUXIO-2045)
      mMasters.get(k).kill();
    }
    LOG.info("Stopping testing zookeeper: {}", mCuratorServer.getConnectString());
    mCuratorServer.stop();
  }
}
