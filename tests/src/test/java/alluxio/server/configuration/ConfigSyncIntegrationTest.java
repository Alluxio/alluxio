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

package alluxio.server.configuration;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.meta.MetaMasterConfigClient;
import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.client.rest.TestCase;
import alluxio.client.rest.TestCaseOptions;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.wire.AlluxioWorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AlluxioWorkerRestServiceHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.HttpMethod;

public class ConfigSyncIntegrationTest extends BaseIntegrationTest {
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;
  private static final int TEST_NUM_MASTERS = 1;
  private static final int TEST_NUM_WORKERS = 1;

  public MultiProcessCluster mCluster;

  @After
  public void after() throws Exception {
    mCluster.destroy();
  }

  @Test
  public void testSyncMasterToWorker() throws Exception {
    PropertyKey testedKey = PropertyKey.WORKER_FREE_SPACE_TIMEOUT;
    Map<Integer, Map<PropertyKey, String>> masterProps = new HashMap<>();
    Map<PropertyKey, String> master0Props = new HashMap<>();
    master0Props.put(PropertyKey.MASTER_UPDATE_CONF_RECORD_ENABLED, "true");
    master0Props.put(PropertyKey.CONF_SYNC_HEARTBEAT_ENABLED, "true");
    master0Props.put(PropertyKey.CONF_SYNC_HEARTBEAT_INTERVAL_MS, "100ms");
    master0Props.put(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED, "true");
    masterProps.put(0, master0Props);
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.CONFIG_CHECKER_MULTI_WORKERS)
        .setClusterName("ConfigCheckerMultiWorkersTest")
        .setMasterProperties(masterProps)
        .setNumMasters(TEST_NUM_MASTERS)
        .setNumWorkers(TEST_NUM_WORKERS)
        .build();

    mCluster.start();
    mCluster.notifySuccess();
    mCluster.waitForAllNodesRegistered(WAIT_TIMEOUT_MS);

    try (MetaMasterConfigClient client = new RetryHandlingMetaMasterConfigClient(
        MasterClientContext.newBuilder(
            mCluster.getFilesystemContext().getClientContext()).build())) {
      client.updateConfiguration(Collections.singletonMap(testedKey, "11sec"));
    }
    List<BlockWorkerInfo> workerList =
        mCluster.getFilesystemContext().getCachedWorkers();
    Assert.assertEquals(1, workerList.size());
    WorkerNetAddress workerNetAddress =
        workerList.get(0).getNetAddress();
    Thread.sleep(200);

    Object actualValue =
        getInfo(workerNetAddress.getHost(),
            workerNetAddress.getWebPort()).getConfiguration().get(testedKey.getName());
    Assert.assertEquals("11sec", actualValue);

    try (MetaMasterConfigClient client = new RetryHandlingMetaMasterConfigClient(
        MasterClientContext.newBuilder(
            mCluster.getFilesystemContext().getClientContext()).build())) {
      client.updateConfiguration(Collections.singletonMap(testedKey, "12sec"));
    }
    Thread.sleep(200);

    AlluxioWorkerInfo alluxioWorkerInfo = getInfo(workerNetAddress.getHost(),
        workerNetAddress.getWebPort());
    Object actualValue2 =
        alluxioWorkerInfo.getConfiguration().get(testedKey.getName());
    Assert.assertEquals("12sec", actualValue2);
    Assert.assertEquals(2,
        alluxioWorkerInfo.getMetrics().get(
            MetricsSystem.getWorkerMetricName(
                MetricKey.UPDATE_CONF_SUCCESS_COUNT.getName())).longValue());
    Assert.assertEquals(0,
        alluxioWorkerInfo.getMetrics().get(
            MetricsSystem.getWorkerMetricName(
                MetricKey.UPDATE_CONF_FAIL_COUNT.getName())).longValue());
  }

  @Test
  public void testSyncMasterToWorker2() throws Exception {
    PropertyKey testedKey = PropertyKey.WORKER_FREE_SPACE_TIMEOUT;
    Map<Integer, Map<PropertyKey, String>> masterProps = new HashMap<>();
    Map<PropertyKey, String> master0Props = new HashMap<>();
    master0Props.put(PropertyKey.MASTER_UPDATE_CONF_RECORD_ENABLED, "true");
    master0Props.put(PropertyKey.CONF_SYNC_HEARTBEAT_ENABLED, "true");
    master0Props.put(PropertyKey.CONF_SYNC_HEARTBEAT_INTERVAL_MS, "100ms");
    master0Props.put(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED, "true");
    master0Props.put(testedKey, "12sec");
    masterProps.put(0, master0Props);
    Map<Integer, Map<PropertyKey, String>> workerProps = new HashMap<>();
    Map<PropertyKey, String> worker0Props = new HashMap<>();
    worker0Props.put(testedKey, "9sec");
    workerProps.put(0, worker0Props);
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.CONFIG_CHECKER_MULTI_WORKERS)
        .setClusterName("ConfigCheckerMultiWorkersTest")
        .setMasterProperties(masterProps)
        .setWorkerProperties(workerProps)
        .setNumMasters(TEST_NUM_MASTERS)
        .setNumWorkers(TEST_NUM_WORKERS)
        .build();

    mCluster.start();
    mCluster.notifySuccess();
    mCluster.waitForAllNodesRegistered(WAIT_TIMEOUT_MS);

    try (MetaMasterConfigClient client = new RetryHandlingMetaMasterConfigClient(
        MasterClientContext.newBuilder(
            mCluster.getFilesystemContext().getClientContext()).build())) {
      client.updateConfiguration(Collections.singletonMap(testedKey, "11sec"));
    }
    List<BlockWorkerInfo> workerList =
        mCluster.getFilesystemContext().getCachedWorkers();
    Assert.assertEquals(1, workerList.size());
    Thread.sleep(200);
    WorkerNetAddress workerNetAddress =
        workerList.get(0).getNetAddress();
    AlluxioWorkerInfo alluxioWorkerInfo = getInfo(workerNetAddress.getHost(),
        workerNetAddress.getWebPort());
    Object actualValue2 =
        alluxioWorkerInfo.getConfiguration().get(testedKey.getName());
    Assert.assertEquals("11sec", actualValue2);
    Assert.assertEquals(1,
            alluxioWorkerInfo.getMetrics().get(
                    MetricsSystem.getWorkerMetricName(
                            MetricKey.UPDATE_CONF_SUCCESS_COUNT.getName())).longValue());
    Assert.assertEquals(0,
            alluxioWorkerInfo.getMetrics().get(
                    MetricsSystem.getWorkerMetricName(
                            MetricKey.UPDATE_CONF_FAIL_COUNT.getName())).longValue());
  }

  private AlluxioWorkerInfo getInfo(String hostname, int port) throws Exception {
    String baseUri = String.format("%s/%s", Constants.REST_API_PREFIX,
        AlluxioWorkerRestServiceHandler.SERVICE_PREFIX);
    String result = new TestCase(hostname, port, baseUri,
        AlluxioWorkerRestServiceHandler.GET_INFO, ImmutableMap.of(), HttpMethod.GET,
        TestCaseOptions.defaults()).runAndGetResponse();
    AlluxioWorkerInfo info = new ObjectMapper().readValue(result, AlluxioWorkerInfo.class);
    return info;
  }
}
