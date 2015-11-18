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

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.thrift.NetAddress;
import tachyon.underfs.UnderFileSystemCluster;
import tachyon.util.CommonUtils;
import tachyon.util.LineageUtils;
import tachyon.util.UnderFileSystemUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockWorker;
import tachyon.worker.lineage.LineageWorker;

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
public final class LocalTachyonCluster extends AbstractLocalTachyonCluster {
  public static void main(String[] args) throws Exception {
    LocalTachyonCluster cluster = new LocalTachyonCluster(100, 8 * Constants.MB, Constants.GB);
    cluster.start();
    CommonUtils.sleepMs(Constants.SECOND_MS);
    cluster.stop();
    CommonUtils.sleepMs(Constants.SECOND_MS);

    cluster = new LocalTachyonCluster(100, 8 * Constants.MB, Constants.GB);
    cluster.start();
    CommonUtils.sleepMs(Constants.SECOND_MS);
    cluster.stop();
    CommonUtils.sleepMs(Constants.SECOND_MS);
  }

  private LocalTachyonMaster mMaster;
  private TachyonConf mClientConf;

  public LocalTachyonCluster(long workerCapacityBytes, int quotaUnitBytes, int userBlockSize) {
    super(workerCapacityBytes, userBlockSize);
    mQuotaUnitBytes = quotaUnitBytes;
  }

  @Override
  public TachyonFileSystem getClient() throws IOException {
    return mMaster.getClient();
  }

  public LocalTachyonMaster getMaster() {
    return mMaster;
  }

  public String getMasterHostname() {
    return mHostname;
  }

  public String getMasterUri() {
    return mMaster.getUri();
  }

  @Override
  public int getMasterPort() {
    return mMaster.getRPCLocalPort();
  }

  public String getTachyonHome() {
    return mTachyonHome;
  }

  public BlockWorker getWorker() {
    return mWorker;
  }

  public LineageWorker getLineageWorker() {
    return mLineageWorker;
  }

  public TachyonConf getWorkerTachyonConf() {
    return mWorkerConf;
  }

  public NetAddress getWorkerAddress() {
    return mWorker.getWorkerNetAddress();
  }

  /**
   * Sets up corresponding directories for tests.
   *
   * @param testConf configuration of this test
   * @throws IOException when creating or deleting dirs failed
   */
  protected void setupTest(TachyonConf testConf) throws IOException {
    String tachyonHome = testConf.get(Constants.TACHYON_HOME);
    // Delete the tachyon home dir for this test from ufs to avoid permission problems
    UnderFileSystemUtils.deleteDir(tachyonHome, testConf);

    // Create ufs dir
    UnderFileSystemUtils.mkdirIfNotExists(testConf.get(Constants.UNDERFS_ADDRESS),
        testConf);

    // Create storage dirs for worker
    int numLevel = testConf.getInt(Constants.WORKER_TIERED_STORE_LEVELS);
    for (int level = 0; level < numLevel; level ++) {
      String tierLevelDirPath =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level);
      String[] dirPaths = testConf.get(tierLevelDirPath).split(",");
      for (String dirPath : dirPaths) {
        UnderFileSystemUtils.mkdirIfNotExists(dirPath, testConf);
      }
    }
  }

  /**
   * Configures and starts master.
   *
   * @throws IOException when the operation fails
   */
  @Override
  protected void startMaster(TachyonConf testConf) throws IOException {
    mMasterConf = new TachyonConf(testConf.getInternalProperties());
    MasterContext.reset(mMasterConf);

    mMaster = LocalTachyonMaster.create(mTachyonHome);
    mMaster.start();

    // Update the test conf with actual RPC port.
    testConf.set(Constants.MASTER_PORT, String.valueOf(getMasterPort()));

    // If we are using the LocalMiniDFSCluster, we need to update the UNDERFS_ADDRESS to point to
    // the cluster's current address. This must happen here because the cluster isn't initialized
    // until mMaster is started.
    UnderFileSystemCluster ufs = UnderFileSystemCluster.get();
    // TODO(andrew): Move logic to the integration-tests project so that we can use instanceof here
    // instead of comparing classnames.
    if (ufs.getClass().getSimpleName().equals("LocalMiniDFSCluster")) {
      String ufsAddress = ufs.getUnderFilesystemAddress() + mTachyonHome;
      testConf.set(Constants.UNDERFS_ADDRESS, ufsAddress);
      MasterContext.getConf().set(Constants.UNDERFS_ADDRESS, ufsAddress);
      mMasterConf = MasterContext.getConf();
    }

    // We need to update client context with the most recent configuration so they know the correct
    // port to connect to master.
    mClientConf = new TachyonConf(testConf.getInternalProperties());
    ClientContext.reset(mClientConf);
  }

  /**
   * Configure and start worker.
   *
   * @throws IOException when the operation fails
   * @throws ConnectionFailedException if network connection failed
   */
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

  /**
   * Stop the tachyon filesystem's service thread only.
   *
   * @throws Exception when the operation fails
   */
  @Override
  public void stopTFS() throws Exception {
    LOG.info("stop Tachyon filesytstem");

    // Stopping Worker before stopping master speeds up tests
    mWorker.stop();
    if (LineageUtils.isLineageEnabled(WorkerContext.getConf())) {
      mLineageWorker.stop();
    }
    mMaster.stop();
  }

  /**
   * Cleanup the underfs cluster test folder only.
   *
   * @throws Exception when the operation fails
   */
  @Override
  public void stopUFS() throws Exception {
    LOG.info("stop under storage system");
    mMaster.cleanupUnderfs();
  }

  /**
   * Cleanup the worker state from the master and stop the worker.
   *
   * @throws Exception when the operation fails
   */
  public void stopWorker() throws Exception {
    mMaster.clearClients();
    mWorker.stop();
    if (LineageUtils.isLineageEnabled(WorkerContext.getConf())) {
      mLineageWorker.stop();
    }
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    // clear HDFS client caching
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }
}
