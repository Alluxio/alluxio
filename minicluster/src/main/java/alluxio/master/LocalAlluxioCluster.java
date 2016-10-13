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

import alluxio.client.file.FileSystem;
import alluxio.exception.ConnectionFailedException;
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
   * Runs a test Alluxio cluster with a single Alluxio worker.
   */
  public LocalAlluxioCluster() {
    super(1);
  }

  /**
   * @param numWorkers the number of workers to run
   */
  public LocalAlluxioCluster(int numWorkers) {
    super(numWorkers);
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
   * @return the RPC port of the master
   */
  public int getMasterRpcPort() {
    return mMaster.getRPCLocalPort();
  }

  /**
   * @return the home path to Alluxio
   */
  public String getAlluxioHome() {
    return mWorkDirectory;
  }

  /**
   * @return the first worker
   */
  public AlluxioWorkerService getWorker() {
    return mWorkers.get(0);
  }

  /**
   * @return the address of the first worker
   */
  public WorkerNetAddress getWorkerAddress() {
    return getWorker().getAddress();
  }

  @Override
  protected void startMaster() throws IOException {
    mMaster = LocalAlluxioMaster.create(mWorkDirectory);
    mMaster.start();
  }

  @Override
  protected void startWorkers() throws IOException, ConnectionFailedException {
    // We need to update the worker context with the most recent configuration so they know the
    // correct port to connect to master.
    runWorkers();
  }

  @Override
  public void stopFS() throws Exception {
    LOG.info("stop Alluxio filesystem");

    // Stopping Workers before stopping master speeds up tests
    for (AlluxioWorkerService worker : mWorkers) {
      worker.stop();
    }
    mMaster.stop();
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    // clear HDFS client caching
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }
}
