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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.exception.ConnectionFailedException;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AlluxioWorkerService;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Local Alluxio cluster for integration tests.
 *
 * Example to use
 * <pre>
 * // Create a cluster instance
 * localAlluxioCluster = new LocalAlluxioCluster(WORKER_CAPACITY_BYTES, BLOCK_SIZE_BYTES);
 * // If you have special conf parameter to set for integration tests:
 * Configuration testConf = localAlluxioCluster.newTestConf();
 * testConf.set(Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES));
 * // After setting up the test conf, start this local cluster:
 * localAlluxioCluster.start(testConf);
 * </pre>
 */
@NotThreadSafe
public final class LocalAlluxioCluster extends AbstractLocalAlluxioCluster {
  private LocalAlluxioMaster mMaster;

  /**
   * @param workerCapacityBytes the capacity of the worker in bytes
   * @param userBlockSize the block size for a user
   */
  public LocalAlluxioCluster(long workerCapacityBytes, int userBlockSize) {
    super(workerCapacityBytes, userBlockSize);
  }

  @Override
  public FileSystem getClient() throws IOException {
    return mMaster.getClient();
  }

  @Override
  public LocalAlluxioMaster getMaster() {
    return mMaster;
  }

  /**
   * @return the hostname of the cluster
   */
  public String getHostname() {
    return mHostname;
  }

  /**
   * @return the URI of the master
   */
  public String getMasterURI() {
    return mMaster.getUri();
  }

  /**
   * @return the port of the master
   */
  public int getMasterPort() {
    return mMaster.getRPCLocalPort();
  }

  /**
   * @return the home path to Alluxio
   */
  public String getAlluxioHome() {
    return mHome;
  }

  /**
   * @return the worker
   */
  public AlluxioWorkerService getWorker() {
    return mWorker;
  }

  /**
   * @return the address of the worker
   */
  public WorkerNetAddress getWorkerAddress() {
    return new WorkerNetAddress()
        .setHost(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC))
        .setRpcPort(Configuration.getInt(PropertyKey.WORKER_RPC_PORT))
        .setDataPort(Configuration.getInt(PropertyKey.WORKER_DATA_PORT))
        .setWebPort(Configuration.getInt(PropertyKey.WORKER_WEB_PORT));
  }

  @Override
  protected void startMaster() throws IOException {
    mMaster = LocalAlluxioMaster.create(mHome);
    mMaster.start();
  }

  @Override
  protected void startWorker() throws IOException, ConnectionFailedException {
    // We need to update the worker context with the most recent configuration so they know the
    // correct port to connect to master.
    runWorker();
  }

  @Override
  public void stopFS() throws Exception {
    LOG.info("stop Alluxio filesystem");

    // Stopping Worker before stopping master speeds up tests
    mWorker.stop();
    mMaster.stop();
  }

  /**
   * Cleans up the worker state from the master and stops the worker.
   *
   * @throws Exception when the operation fails
   */
  public void stopWorker() throws Exception {
    mMaster.clearClients();
    mWorker.stop();
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    // clear HDFS client caching
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }
}
