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

import javax.annotation.concurrent.NotThreadSafe;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.file.FileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.worker.NetAddress;
import tachyon.worker.TachyonWorker;
import tachyon.worker.WorkerContext;

/**
 * Local Tachyon cluster for integration tests.
 *
 * Example to use
 * <pre>
 * // Create a cluster instance
 * localTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES,
 *     QUOTA_UNIT_BYTES, BLOCK_SIZE_BYTES);
 * // If you have special conf parameter to set for integration tests:
 * TachyonConf testConf = localTachyonCluster.newTestConf();
 * testConf.set(Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES));
 * // After setting up the test conf, start this local cluster:
 * localTachyonCluster.start(testConf);
 * </pre>
 */
@NotThreadSafe
public final class LocalTachyonCluster extends AbstractLocalTachyonCluster {
  private LocalTachyonMaster mMaster;
  private TachyonConf mClientConf;

  /**
   * @param workerCapacityBytes the capacity of the worker in bytes
   * @param userBlockSize the block size for a user
   */
  public LocalTachyonCluster(long workerCapacityBytes, int userBlockSize) {
    super(workerCapacityBytes, userBlockSize);
  }

  @Override
  public FileSystem getClient() throws IOException {
    return mMaster.getClient();
  }

  @Override
  public LocalTachyonMaster getMaster() {
    return mMaster;
  }

  /**
   * @return the hostname of the master
   */
  public String getMasterHostname() {
    return mHostname;
  }

  /**
   * @return the URI of the master
   */
  public String getMasterUri() {
    return mMaster.getUri();
  }

  /**
   * @return the port of the master
   */
  public int getMasterPort() {
    return mMaster.getRPCLocalPort();
  }

  /**
   * @return the home path to Tachyon
   */
  public String getTachyonHome() {
    return mTachyonHome;
  }

  /**
   * @return the worker
   */
  public TachyonWorker getWorker() {
    return mWorker;
  }

  /**
   * @return the configuration for Tachyon
   */
  public TachyonConf getWorkerTachyonConf() {
    return mWorkerConf;
  }

  /**
   * @return the address of the worker
   */
  public NetAddress getWorkerAddress() {
    return mWorker.getNetAddress();
  }

  @Override
  protected void startMaster(TachyonConf testConf) throws IOException {
    mMasterConf = new TachyonConf(testConf.getInternalProperties());
    MasterContext.reset(mMasterConf);

    mMaster = LocalTachyonMaster.create(mTachyonHome);
    mMaster.start();

    // Update the test conf with actual RPC port.
    testConf.set(Constants.MASTER_RPC_PORT, String.valueOf(getMasterPort()));

    // We need to update client context with the most recent configuration so they know the correct
    // port to connect to master.
    mClientConf = new TachyonConf(testConf.getInternalProperties());
    ClientContext.reset(mClientConf);
  }

  @Override
  protected void startWorker(TachyonConf testConf) throws IOException, ConnectionFailedException  {
    // We need to update the worker context with the most recent configuration so they know the
    // correct port to connect to master.
    mWorkerConf = new TachyonConf(testConf.getInternalProperties());
    WorkerContext.reset(mWorkerConf);

    runWorker();
  }

  @Override
  protected void resetContext() {
    MasterContext.reset();
    WorkerContext.reset();
    ClientContext.reset();
  }

  @Override
  public void stopTFS() throws Exception {
    LOG.info("stop Tachyon filesystem");

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
