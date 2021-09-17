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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.WritePType;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.meta.AlluxioMasterRestServiceHandler;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthType;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.UnderFileSystemTestUtils;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.AlluxioMasterInfo;
import alluxio.wire.Capacity;
import alluxio.wire.MountPointInfo;
import alluxio.wire.WorkerInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioMasterRestServiceHandler}.
 */
public final class AlluxioMasterRestApiTest extends RestApiTest {
  private FileSystemMaster mFileSystemMaster;

  // TODO(chaomin): Rest API integration tests are only run in NOSASL mode now. Need to
  // fix the test setup in SIMPLE mode.
  @ClassRule
  public static LocalAlluxioClusterResource sResource = new LocalAlluxioClusterResource.Builder()
      .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false")
      .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName())
      .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, "1KB").build();

  @Rule
  public TestRule mResetRule = sResource.getResetResource();

  @Before
  public void before() {
    mFileSystemMaster = sResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
    mHostname = sResource.get().getHostname();
    mPort = sResource.get().getLocalAlluxioMaster().getMasterProcess().getWebAddress().getPort();
    mServicePrefix = AlluxioMasterRestServiceHandler.SERVICE_PREFIX;
  }

  private AlluxioMasterInfo getInfo(Map<String, String> params) throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_INFO),
            params, HttpMethod.GET, null).call();
    AlluxioMasterInfo info = new ObjectMapper().readValue(result, AlluxioMasterInfo.class);
    return info;
  }

  private Map<String, String> getMetrics(Map<String, String> params) throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_METRICS),
            params, HttpMethod.GET, null).call();
    Map<String, String> info = new ObjectMapper().readValue(result, Map.class);
    return info;
  }

  @Test
  public void getCapacity() throws Exception {
    long total = ServerConfiguration.getBytes(PropertyKey.WORKER_RAMDISK_SIZE);
    Capacity capacity = getInfo(NO_PARAMS).getCapacity();
    assertEquals(total, capacity.getTotal());
    assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getConfiguration() throws Exception {
    String home = "home";
    String rawConfDir = String.format("${%s}/conf", PropertyKey.Name.HOME);
    String resolvedConfDir = String.format("%s/conf", home);
    ServerConfiguration.set(PropertyKey.HOME, home);
    ServerConfiguration.set(PropertyKey.CONF_DIR, rawConfDir);

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
  public void getMetricsInfo() throws Exception {
    long start = getInfo(NO_PARAMS).getMetrics()
        .get(MetricsSystem.getMetricName(MetricKey.MASTER_FILE_INFOS_GOT.getName()));
    mFileSystemMaster.getFileInfo(new AlluxioURI("/"), GetStatusContext.defaults());
    assertEquals(Long.valueOf(start + 1), getInfo(NO_PARAMS).getMetrics()
        .get(MetricsSystem.getMetricName(MetricKey.MASTER_FILE_INFOS_GOT.getName())));
  }

  @Test
  public void getUfsMetrics() throws Exception {
    int len = 100;
    FileSystemTestUtils.createByteFile(sResource.get().getClient(), "/f1", WritePType.THROUGH, len);
    CommonUtils.waitFor("Metrics to be updated correctly", () -> {
      try {
        return FormatUtils.getSizeFromBytes(len).equals(getMetrics(NO_PARAMS)
            .get("totalBytesWrittenUfs"));
      } catch (Exception e) {
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(2000));

    FileSystemTestUtils.createByteFile(sResource.get().getClient(), "/f2", WritePType.THROUGH, len);
    CommonUtils.waitFor("Metrics to be updated correctly", () -> {
      try {
        return FormatUtils.getSizeFromBytes(2 * len).equals(getMetrics(NO_PARAMS)
            .get("totalBytesWrittenUfs"));
      } catch (Exception e) {
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(2000));
  }

  @Test
  public void getMountPoints() throws Exception {
    Map<String, MountPointInfo> mountTable = mFileSystemMaster.getMountPointInfoSummary();
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
        .contains(String.valueOf(NetworkAddressUtils.getPort(ServiceType.MASTER_RPC,
            ServerConfiguration.global()))));
  }

  @Test
  public void getStartTimeMs() throws Exception {
    assertTrue(getInfo(NO_PARAMS).getStartTimeMs() > 0);
  }

  @Test
  public void getTierCapacity() throws Exception {
    long total = ServerConfiguration.getBytes(PropertyKey.WORKER_RAMDISK_SIZE);
    Capacity capacity = getInfo(NO_PARAMS).getTierCapacity().get(Constants.MEDIUM_MEM);
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
    long bytes = ServerConfiguration.getBytes(PropertyKey.WORKER_RAMDISK_SIZE);
    assertEquals(bytes, workerInfo.getCapacityBytes());
  }

  @Test
  public void getVersion() throws Exception {
    assertEquals(RuntimeConstants.VERSION, getInfo(NO_PARAMS).getVersion());
  }
}
