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

import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.membership.WorkerClusterView;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.WorkerProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
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
  private boolean mIncludeProxy;

  private LocalAlluxioMaster mMaster;

  /**
   * Runs a test Alluxio cluster with a single Alluxio worker.
   */
  public LocalAlluxioCluster() {
    this(1, false);
  }

  /**
   * @param numWorkers the number of workers to run
   * @param includeProxy weather to include the proxy
   */
  public LocalAlluxioCluster(int numWorkers, boolean includeProxy) {
    super(numWorkers);
    mIncludeProxy = includeProxy;
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
    for (Map.Entry<PropertyKey, Object> entry : ConfigurationTestUtils
        .testConfigurationDefaults(Configuration.global(),
            mHostname, mWorkDirectory).entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }
    Configuration.set(PropertyKey.TEST_MODE, true);
    Configuration.set(PropertyKey.JOB_WORKER_THROTTLING, false);
    Configuration.set(PropertyKey.PROXY_WEB_PORT, 0);
    Configuration.set(PropertyKey.WORKER_RPC_PORT, 0);
    Configuration.set(PropertyKey.WORKER_DATA_PORT, 0);
    Configuration.set(PropertyKey.WORKER_WEB_PORT, 0);
    Configuration.set(PropertyKey.WORKER_HTTP_SERVER_PORT, 0);
    Configuration.set(PropertyKey.WORKER_REST_PORT, 0);
  }

  @Override
  public void startMasters() throws Exception {
    mMaster = LocalAlluxioMaster.create(mWorkDirectory);
    mMaster.start();
    waitForMasterServing();
  }

  @Override
  protected void startProxy() throws Exception {
    if (mIncludeProxy) {
      super.startProxy();
    }
  }

  @Override
  protected void waitForMasterServing() throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("master starts serving RPCs", () -> {
      try (BlockMasterClient blockMasterClient = BlockMasterClient.Factory.create(
          // This cluster uses the global configuration singleton instead of a local one
          // Config properties are only set in the initConfiguration() method
          MasterClientContext.newBuilder(ClientContext.create(Configuration.global())).build())) {
        List<WorkerInfo> workerInfoList = blockMasterClient.getWorkerInfoList();
        return true;
      } catch (IOException ioe) {
        LOG.error("getWorkerInfoList() ERROR: ", ioe);
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10_000));
  }

  @Override
  protected void waitForWorkersServing() throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("worker starts serving RPCs", () -> {
      try (FileSystemContext fsContext = FileSystemContext.create()) {
        WorkerClusterView workers = fsContext.getCachedWorkers();
        LOG.info("Observed {} workers in the cluster", workers.size());
        return workers.size() == mNumWorkers;
      } catch (IOException ioe) {
        LOG.error(ioe.getMessage());
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10_000));
  }

  @Override
  public void stop() throws Exception {
    LOG.info("stop local alluxio cluster.");
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
