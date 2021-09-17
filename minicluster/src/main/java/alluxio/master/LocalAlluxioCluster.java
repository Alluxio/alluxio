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
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.WorkerProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

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
  public static final String DEFAULT_TEST_NAME = "test";

  private static final Logger LOG = LoggerFactory.getLogger(LocalAlluxioCluster.class);

  private boolean mIncludeSecondary;

  private LocalAlluxioMaster mMaster;

  /**
   * Runs a test Alluxio cluster with a single Alluxio worker.
   */
  public LocalAlluxioCluster() {
    this(1, false);
  }

  /**
   * @param numWorkers the number of workers to run
   * @param includeSecondary weather to include the secondary master
   */
  public LocalAlluxioCluster(int numWorkers, boolean includeSecondary) {
    super(numWorkers);
    mIncludeSecondary = includeSecondary;
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
  public void initConfiguration(String name) throws IOException {
    setAlluxioWorkDirectory(name);
    setHostname();
    for (Map.Entry<PropertyKey, String> entry : ConfigurationTestUtils
        .testConfigurationDefaults(ServerConfiguration.global(),
            mHostname, mWorkDirectory).entrySet()) {
      ServerConfiguration.set(entry.getKey(), entry.getValue());
    }
    ServerConfiguration.set(PropertyKey.TEST_MODE, true);
    ServerConfiguration.set(PropertyKey.JOB_WORKER_THROTTLING, false);
    ServerConfiguration.set(PropertyKey.PROXY_WEB_PORT, 0);
    ServerConfiguration.set(PropertyKey.WORKER_RPC_PORT, 0);
    ServerConfiguration.set(PropertyKey.WORKER_WEB_PORT, 0);
  }

  @Override
  public void startMasters() throws Exception {
    mMaster = LocalAlluxioMaster.create(mWorkDirectory, mIncludeSecondary);
    mMaster.start();
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    TestUtils.assertAllLocksReleased(this);
    // clear HDFS client caching
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }

  @Override
  public void stopMasters() throws Exception {
    mMaster.stop();
  }
}
