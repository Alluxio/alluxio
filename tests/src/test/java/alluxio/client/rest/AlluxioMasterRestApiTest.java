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

package alluxio.client.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.meta.AlluxioMasterRestServiceHandler;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.StartupConsistencyCheck;
import alluxio.metrics.MetricsSystem;
import alluxio.testutils.master.MasterTestUtils;
import alluxio.testutils.underfs.UnderFileSystemTestUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.AlluxioMasterInfo;
import alluxio.wire.Capacity;
import alluxio.wire.MountPointInfo;
import alluxio.wire.WorkerInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioMasterRestServiceHandler}.
 */
public final class AlluxioMasterRestApiTest extends RestApiTest {
  private FileSystemMaster mFileSystemMaster;

  @Before
  public void before() {
    mFileSystemMaster = mResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getLocalAlluxioMaster().getMasterProcess().getWebAddress().getPort();
    mServicePrefix = AlluxioMasterRestServiceHandler.SERVICE_PREFIX;

    MetricsSystem.resetAllCounters();
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  private AlluxioMasterInfo getInfo(Map<String, String> params) throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_INFO),
            params, HttpMethod.GET, null).call();
    AlluxioMasterInfo info = new ObjectMapper().readValue(result, AlluxioMasterInfo.class);
    return info;
  }

  @Test
  public void getCapacity() throws Exception {
    long total = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo(NO_PARAMS).getCapacity();
    assertEquals(total, capacity.getTotal());
    assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getConfiguration() throws Exception {
    String home = "home";
    String rawConfDir = String.format("${%s}/conf", PropertyKey.Name.HOME);
    String resolvedConfDir = String.format("%s/conf", home);
    Configuration.set(PropertyKey.HOME, home);
    Configuration.set(PropertyKey.CONF_DIR, rawConfDir);

    // with out any query parameter, configuration values are resolved.
    checkConfiguration(PropertyKey.CONF_DIR, resolvedConfDir, NO_PARAMS);

    // with QUERY_RAW_CONFIGURATION=false, configuration values are resolved.
    Map<String, String> params = new HashMap<>();
    params.put(AlluxioMasterRestServiceHandler.QUERY_RAW_CONFIGURATION, "false");
    checkConfiguration(PropertyKey.CONF_DIR, resolvedConfDir, params);

    // with QUERY_RAW_CONFIGURATION=true, configuration values are raw.
    params.put(AlluxioMasterRestServiceHandler.QUERY_RAW_CONFIGURATION, "true");
    checkConfiguration(PropertyKey.CONF_DIR, rawConfDir, params);
  }

  private void checkConfiguration(PropertyKey key, String expectedValue, Map<String, String> params)
      throws Exception {
    assertEquals(expectedValue, getInfo(params).getConfiguration().get(key.toString()));
  }

  @Test
  public void getLostWorkers() throws Exception {
    List<WorkerInfo> lostWorkersInfo = getInfo(NO_PARAMS).getLostWorkers();
    assertEquals(0, lostWorkersInfo.size());
  }

  @Test
  public void getMetrics() throws Exception {
    assertEquals(Long.valueOf(0), getInfo(NO_PARAMS).getMetrics()
        .get("master.CompleteFileOps"));
  }

  @Test
  public void getMountPoints() throws Exception {
    Map<String, MountPointInfo> mountTable = mFileSystemMaster.getMountTable();
    Map<String, MountPointInfo> mountPoints = getInfo(NO_PARAMS).getMountPoints();
    assertEquals(mountTable.size(), mountPoints.size());
    for (Map.Entry<String, MountPointInfo> mountPoint : mountTable.entrySet()) {
      assertTrue(mountPoints.containsKey(mountPoint.getKey()));
      String expectedUri = mountPoints.get(mountPoint.getKey()).getUfsUri();
      String returnedUri = mountPoint.getValue().getUfsUri();
      assertEquals(expectedUri, returnedUri);
    }
  }

  @Test
  public void getRpcAddress() throws Exception {
    assertTrue(getInfo(NO_PARAMS).getRpcAddress()
        .contains(String.valueOf(NetworkAddressUtils.getPort(ServiceType.MASTER_RPC))));
  }

  @Test
  public void getStartTimeMs() throws Exception {
    assertTrue(getInfo(NO_PARAMS).getStartTimeMs() > 0);
  }

  @Test
  public void getStartupConsistencyCheckStatus() throws Exception {
    MasterTestUtils.waitForStartupConsistencyCheck(mFileSystemMaster);
    alluxio.wire.StartupConsistencyCheck status = getInfo(NO_PARAMS)
        .getStartupConsistencyCheck();
    assertEquals(
        StartupConsistencyCheck.Status.COMPLETE.toString().toLowerCase(),
        status.getStatus());
    assertEquals(0, status.getInconsistentUris().size());
  }

  @Test
  public void getTierCapacity() throws Exception {
    long total = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo(NO_PARAMS).getTierCapacity().get("MEM");
    assertEquals(total, capacity.getTotal());
    assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getUptimeMs() throws Exception {
    assertTrue(getInfo(NO_PARAMS).getUptimeMs() > 0);
  }

  @Test
  public void getUfsCapacity() throws Exception {
    Capacity ufsCapacity = getInfo(NO_PARAMS).getUfsCapacity();
    if (UnderFileSystemTestUtils.isObjectStorage(mFileSystemMaster.getUfsAddress())) {
      // Object storage ufs capacity is always invalid.
      assertEquals(-1, ufsCapacity.getTotal());
    } else {
      assertTrue(ufsCapacity.getTotal() > 0);
    }
  }

  @Test
  public void getWorkers() throws Exception {
    List<WorkerInfo> workerInfos = getInfo(NO_PARAMS).getWorkers();
    assertEquals(1, workerInfos.size());
    WorkerInfo workerInfo = workerInfos.get(0);
    assertEquals(0, workerInfo.getUsedBytes());
    long bytes = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    assertEquals(bytes, workerInfo.getCapacityBytes());
  }

  @Test
  public void getVersion() throws Exception {
    assertEquals(RuntimeConstants.VERSION, getInfo(NO_PARAMS).getVersion());
  }
}
