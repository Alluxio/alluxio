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
import alluxio.RuntimeConstants;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.metrics.MetricsSystem;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.AlluxioMasterInfo;
import alluxio.wire.Capacity;
import alluxio.wire.MountPointInfo;
import alluxio.wire.WorkerInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
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
    mFileSystemMaster =
        mResource.get().getMaster().getInternalMaster().getMaster(FileSystemMaster.class);
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getMaster().getInternalMaster().getWebAddress().getPort();
    mServicePrefix = AlluxioMasterRestServiceHandler.SERVICE_PREFIX;

    MetricsSystem.resetAllCounters();
  }

  @After
  public void after() {
    // Reset Configuration in case some properties are set to custom values during the tests,
    // e.g. getConfiguration(). Since JVM is shared among tests, if this is not reset, the
    // changed properties will affect other tests.
    Configuration.defaultInit();
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
    Assert.assertEquals(total, capacity.getTotal());
    Assert.assertEquals(0, capacity.getUsed());
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
    Assert.assertEquals(expectedValue, getInfo(params).getConfiguration().get(key.toString()));
  }

  @Test
  public void getLostWorkers() throws Exception {
    List<WorkerInfo> lostWorkersInfo = getInfo(NO_PARAMS).getLostWorkers();
    Assert.assertEquals(0, lostWorkersInfo.size());
  }

  @Test
  public void getMetrics() throws Exception {
    Assert.assertEquals(Long.valueOf(0), getInfo(NO_PARAMS).getMetrics()
        .get("master.CompleteFileOps"));
  }

  @Test
  public void getMountPoints() throws Exception {
    Map<String, MountInfo> mountTable = mFileSystemMaster.getMountTable();
    Map<String, MountPointInfo> mountPoints = getInfo(NO_PARAMS).getMountPoints();
    Assert.assertEquals(mountTable.size(), mountPoints.size());
    for (Map.Entry<String, MountInfo> mountPoint : mountTable.entrySet()) {
      Assert.assertTrue(mountPoints.containsKey(mountPoint.getKey()));
      String expectedUri = mountPoints.get(mountPoint.getKey()).getUfsUri();
      String returnedUri = mountPoint.getValue().getUfsUri().toString();
      Assert.assertEquals(expectedUri, returnedUri);
    }
  }

  @Test
  public void getRpcAddress() throws Exception {
    Assert.assertTrue(getInfo(NO_PARAMS).getRpcAddress()
        .contains(String.valueOf(NetworkAddressUtils.getPort(ServiceType.MASTER_RPC))));
  }

  @Test
  public void getStartTimeMs() throws Exception {
    Assert.assertTrue(getInfo(NO_PARAMS).getStartTimeMs() > 0);
  }

  @Test
  public void getStartupConsistencyCheckStatus() throws Exception {
    MasterTestUtils.waitForStartupConsistencyCheck(mFileSystemMaster);
    alluxio.wire.StartupConsistencyCheck status = getInfo(NO_PARAMS)
        .getStartupConsistencyCheck();
    Assert.assertEquals(
        FileSystemMaster.StartupConsistencyCheck.Status.COMPLETE.toString().toLowerCase(),
        status.getStatus());
    Assert.assertEquals(0, status.getInconsistentUris().size());
  }

  @Test
  public void getTierCapacity() throws Exception {
    long total = Configuration.getLong(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo(NO_PARAMS).getTierCapacity().get("MEM");
    Assert.assertEquals(total, capacity.getTotal());
    Assert.assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getUptimeMs() throws Exception {
    Assert.assertTrue(getInfo(NO_PARAMS).getUptimeMs() > 0);
  }

  @Test
  public void getUfsCapacity() throws Exception {
    Capacity ufsCapacity = getInfo(NO_PARAMS).getUfsCapacity();
    if (UnderFileSystemUtils.isObjectStorage(mFileSystemMaster.getUfsAddress())) {
      // Object storage ufs capacity is always invalid.
      Assert.assertEquals(-1, ufsCapacity.getTotal());
    } else {
      Assert.assertTrue(ufsCapacity.getTotal() > 0);
    }
  }

  @Test
  public void getWorkers() throws Exception {
    List<WorkerInfo> workerInfos = getInfo(NO_PARAMS).getWorkers();
    Assert.assertEquals(1, workerInfos.size());
    WorkerInfo workerInfo = workerInfos.get(0);
    Assert.assertEquals(0, workerInfo.getUsedBytes());
    long bytes = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Assert.assertEquals(bytes, workerInfo.getCapacityBytes());
  }

  @Test
  public void getVersion() throws Exception {
    Assert.assertEquals(RuntimeConstants.VERSION, getInfo(NO_PARAMS).getVersion());
  }
}
