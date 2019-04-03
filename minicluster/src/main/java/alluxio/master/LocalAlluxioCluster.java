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

import alluxio.ConfigurationTestUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalType;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.WorkerProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Local Alluxio cluster for integration tests.
 *
 * Example to use
 * <pre>
 * // Create a cluster instance
 * localAlluxioCluster = new LocalAlluxioCluster(WORKER_CAPACITY_BYTES, BLOCK_SIZE_BYTES);
 * // If you have special conf parameter to set for integration tests:
 * AlluxioConfiguration testConf = localAlluxioCluster.newTestConf();
 * testConf.set(Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES));
 * // After setting up the test conf, start this local cluster:
 * localAlluxioCluster.start(testConf);
 * </pre>
 */
@NotThreadSafe
public final class LocalAlluxioCluster extends AbstractLocalAlluxioCluster {
  private static final Logger LOG = LoggerFactory.getLogger(LocalAlluxioCluster.class);

  private LocalAlluxioMaster mMaster;

  private JournalType mJournalType;

  /**
   * Runs a test Alluxio cluster with a single Alluxio worker.
   *
   * @param journalType the journal type to set in the cluster
   */
  public LocalAlluxioCluster(JournalType journalType) {
    this(1, journalType);
  }

  /**
   * @param numWorkers the number of workers to run
   * @param journalType the journal type to set in the cluster
   */
  public LocalAlluxioCluster(int numWorkers, JournalType journalType) {
    super(numWorkers);
    mJournalType = journalType;
  }

  @Override
  public FileSystem getClient() throws IOException {
    return mMaster.getClient();
  }

  @Override
  public FileSystem getClient(FileSystemContext context) throws IOException {
    return mMaster.getClient(context);
  }

  @Override
  public LocalAlluxioMaster getLocalAlluxioMaster() {
    return mMaster;
  }

  @Override
  public void waitForMasterServing(int timeoutMs)
      throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("Master process is serving", () -> {
      try {
        return mMaster.isServing();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setInterval(200).setTimeoutMs(timeoutMs));
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
    return mMaster.getRpcLocalPort();
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
  public WorkerProcess getWorkerProcess() {
    return mWorkers.get(0);
  }

  /**
   * @return the address of the first worker
   */
  public WorkerNetAddress getWorkerAddress() {
    return getWorkerProcess().getAddress();
  }

  @Override
  public void initConfiguration() throws IOException {
    setAlluxioWorkDirectory();
    setHostname();
    for (Map.Entry<PropertyKey, String> entry : ConfigurationTestUtils
        .testConfigurationDefaults(ServerConfiguration.global(), mHostname, mWorkDirectory,
            mJournalType.toString()).entrySet()) {
      ServerConfiguration.set(entry.getKey(), entry.getValue());
    }
    if (mJournalType.equals(JournalType.UFS)) {
      ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TYPE, "UFS");
    }
    ServerConfiguration.set(PropertyKey.TEST_MODE, true);
    ServerConfiguration.set(PropertyKey.PROXY_WEB_PORT, 0);
    ServerConfiguration.set(PropertyKey.WORKER_RPC_PORT, 0);
    ServerConfiguration.set(PropertyKey.WORKER_WEB_PORT, 0);
  }

  @Override
  public void startMasters() throws Exception {
    mMaster = LocalAlluxioMaster.create(mWorkDirectory, true);
    mMaster.start();
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    // clear HDFS client caching
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }

  @Override
  public void stopMasters() throws Exception {
    mMaster.stop();
  }
}
