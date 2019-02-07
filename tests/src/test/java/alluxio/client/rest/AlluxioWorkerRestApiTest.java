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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.AlluxioWorkerInfo;
import alluxio.wire.Capacity;
import alluxio.wire.WorkerWebUIBlockInfo;
import alluxio.wire.WorkerWebUIInit;
import alluxio.wire.WorkerWebUILogs;
import alluxio.wire.WorkerWebUIMetrics;
import alluxio.wire.WorkerWebUIOverview;
import alluxio.worker.AlluxioWorkerRestServiceHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioWorkerRestServiceHandler}.
 */
public final class AlluxioWorkerRestApiTest extends RestApiTest {
  int mMasterWebPort;
  int mWorkerPort;
  String mWorkerAddress;
  String mWorkerStartTime;

  @Before
  public void before() {
    mHostname = mResource.get().getHostname();
    mWorkerPort = mResource.get().getWorkerProcess().getDataLocalPort();
    mWorkerAddress = mResource.get().getWorkerProcess().getAddress().getHost();
    mPort = mResource.get().getWorkerProcess().getWebLocalPort();
    mWorkerStartTime = CommonUtils
        .convertMsToDate(mResource.get().getWorkerProcess().getStartTimeMs(),
            ServerConfiguration.get(PropertyKey.USER_DATE_FORMAT_PATTERN));
    mMasterWebPort =
        mResource.get().getLocalAlluxioMaster().getMasterProcess().getWebAddress().getPort();
    mServicePrefix = AlluxioWorkerRestServiceHandler.SERVICE_PREFIX;
  }

  private AlluxioWorkerInfo getInfo() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_INFO),
            NO_PARAMS, HttpMethod.GET, null).call();
    AlluxioWorkerInfo info = new ObjectMapper().readValue(result, AlluxioWorkerInfo.class);
    return info;
  }

  @Test
  public void getCapacity() throws Exception {
    long total = ServerConfiguration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo().getCapacity();
    assertEquals(total, capacity.getTotal());
    assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getConfiguration() throws Exception {
    ServerConfiguration.set(PropertyKey.METRICS_CONF_FILE, "abc");
    assertEquals("abc",
        getInfo().getConfiguration().get(PropertyKey.METRICS_CONF_FILE.toString()));
  }

  @Test
  public void getMetrics() throws Exception {
    assertEquals(Long.valueOf(0),
        getInfo().getMetrics().get(MetricsSystem.getMetricName("CompleteFileOps")));
  }

  @Test
  public void getRpcAddress() throws Exception {
    assertTrue(
        NetworkAddressUtils.parseInetSocketAddress(getInfo().getRpcAddress()).getPort() > 0);
  }

  @Test
  public void getStartTimeMs() throws Exception {
    assertTrue(getInfo().getStartTimeMs() > 0);
  }

  @Test
  public void getTierCapacity() throws Exception {
    long total = ServerConfiguration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo().getTierCapacity().get("MEM");
    assertEquals(total, capacity.getTotal());
    assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getTierPaths() throws Exception {
    assertTrue(getInfo().getTierPaths().containsKey("MEM"));
  }

  @Test
  public void getUptimeMs() throws Exception {
    assertTrue(getInfo().getUptimeMs() > 0);
  }

  @Test
  public void getVersion() throws Exception {
    assertEquals(RuntimeConstants.VERSION, getInfo().getVersion());
  }

  private WorkerWebUIInit getWebUIInitData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.WEBUI_INIT),
            NO_PARAMS, HttpMethod.GET, null).call();
    WorkerWebUIInit data = new ObjectMapper().readValue(result, WorkerWebUIInit.class);
    return data;
  }

  @Test
  public void WebUIInit() throws Exception {
    WorkerWebUIInit result = getWebUIInitData();
    String expectedJson = ("{" + "`debug`:false, " + "`webFileInfoEnabled`:true, "
        + "`securityAuthorizationPermissionEnabled`:false, " + "`masterHostname`:`" + mHostname
        + "`, `masterPort`:" + mMasterWebPort + ", " + "`refreshInterval`:15000" + "}")
        .replace('`', '"');
    WorkerWebUIInit expected = new ObjectMapper().readValue(expectedJson, WorkerWebUIInit.class);
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
    String resultString = new ObjectMapper().writer().writeValueAsString(result);
    assertEquals(expectedString, resultString);
  }

  private WorkerWebUIOverview getWebUIOverviewData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.WEBUI_OVERVIEW),
            NO_PARAMS, HttpMethod.GET, null).call();
    WorkerWebUIOverview data = new ObjectMapper().readValue(result, WorkerWebUIOverview.class);
    return data;
  }

  @Test
  public void WebUIOverview() throws Exception {
    WorkerWebUIOverview result = getWebUIOverviewData();
    String expectedJson = ("{`capacityBytes`:`100.00MB`,`storageDirs`:[{`tierAlias`:`MEM`,"
        + "`dirPath`:`/var/folders/zk/l642b_mn4097j57qt1wgk3mm0000gn/T/alluxio-tests/test"
        + "-cluster-e5325b1c-7dad-457e-b27f-e06797f109d1/ramdisk/alluxioworker`,"
        + "`capacityBytes`:104857600,`usedBytes`:0}],`usageOnTiers`:[{`tierAlias`:`MEM`,"
        + "`capacityBytes`:104857600,`usedBytes`:0}],`usedBytes`:`0B`,"
        + "`workerInfo`:{`workerAddress`:`/" + mWorkerAddress + ":" + mWorkerPort
        + "`,`startTime`:`" + mWorkerStartTime + "`},`version`:`2.0.0-SNAPSHOT`}")
        .replace('`', '"');
    WorkerWebUIOverview expected =
        new ObjectMapper().readValue(expectedJson, WorkerWebUIOverview.class);
    String findIgnored = "(test-cluster-).{36}";
    String replaceIgnored = "$1";
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected)
        .replaceAll(findIgnored, replaceIgnored);
    String resultString = new ObjectMapper().writer().writeValueAsString(result)
        .replaceAll(findIgnored, replaceIgnored);
    assertEquals(expectedString, resultString);
  }

  private WorkerWebUIBlockInfo getWebUIBlockInfoData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.WEBUI_BLOCKINFO),
            NO_PARAMS, HttpMethod.GET, null).call();
    WorkerWebUIBlockInfo data = new ObjectMapper().readValue(result, WorkerWebUIBlockInfo.class);
    return data;
  }

  @Test
  public void WebUIBlockInfo() throws Exception {
    WorkerWebUIBlockInfo result = getWebUIBlockInfoData();
    String expectedJson = ("{`blockSizeBytes`:null, `fatalError`:``, `fileBlocksOnTier`:null, "
        + "`invalidPathError`:``, `ntotalFile`:0, `fileInfos`:[], "
        + "`orderedTierAliases`:[`MEM`], `path`:null}").replace('`', '"');
    WorkerWebUIBlockInfo expected =
        new ObjectMapper().readValue(expectedJson, WorkerWebUIBlockInfo.class);
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
    String resultString = new ObjectMapper().writer().writeValueAsString(result);
    assertEquals(expectedString, resultString);
  }

  private WorkerWebUIMetrics getWebUIMetricsData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.WEBUI_METRICS),
            NO_PARAMS, HttpMethod.GET, null).call();
    WorkerWebUIMetrics data = new ObjectMapper().readValue(result, WorkerWebUIMetrics.class);
    return data;
  }

  @Test
  public void WebUIMetrics() throws Exception {
    WorkerWebUIMetrics result = getWebUIMetricsData();
    String expectedJson = ("{`workerCapacityUsedPercentage`:0,`workerCapacityFreePercentage`:100,"
        + "`operationMetrics`:{`AsyncCacheDuplicateRequests`:{`value`:0},"
        + "`AsyncCacheFailedBlocks`:{`value`:0},`AsyncCacheRemoteBlocks`:{`value`:0},"
        + "`AsyncCacheRequests`:{`value`:0},`AsyncCacheSucceededBlocks`:{`value`:0},"
        + "`AsyncCacheUfsBlocks`:{`value`:0},`BlocksAccessed`:{`value`:0},"
        + "`BlocksCanceled`:{`value`:0},`BlocksDeleted`:{`value`:0},"
        + "`BlocksEvicted`:{`value`:0},`BlocksLost`:{`value`:0},`BlocksPromoted`:{`value`:0},"
        + "`DirectoriesCreated`:{`value`:0},`FileBlockInfosGot`:{`value`:0},"
        + "`FileInfosGot`:{`value`:1},`FilesCompleted`:{`value`:0},"
        + "`FilesCreated`:{`value`:0},`FilesFreed`:{`value`:0},`FilesPersisted`:{`value`:0},"
        + "`FilesPinned`:{`value`:0},`NewBlocksGot`:{`value`:0},`PathsDeleted`:{`value`:0},"
        + "`PathsMounted`:{`value`:0},`PathsRenamed`:{`value`:0}," + "`PathsUnmounted`:{`value`:0},"
        + "`UfsSessionCount-Ufs:_var_folders_zk_l642b_mn4097j57qt1wgk3mm0000gn_T_alluxio"
        + "-tests_test-cluster-8af603df-b301-4d01-ad2e-515a23aa30e2_underFSStorage`:{`value"
        + "`:0}},`rpcInvocationMetrics`:{`CompleteFileOps`:0,`CreateDirectoryOps`:0,"
        + "`CreateFileOps`:0,`DeletePathOps`:0,`FreeFileOps`:0,`GetFileBlockInfoOps`:0,"
        + "`GetFileInfoOps`:1,`GetNewBlockOps`:0,`MountOps`:0,`RenamePathOps`:0,"
        + "`SetAclOps`:0,`SetAttributeOps`:0,`UnmountOps`:0}}").replace('`', '"');
    WorkerWebUIMetrics expected =
        new ObjectMapper().readValue(expectedJson, WorkerWebUIMetrics.class);
    String findIgnored = "(test-cluster-).{36}";
    String replaceIgnored = "$1";
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected)
        .replaceAll(findIgnored, replaceIgnored);
    String resultString = new ObjectMapper().writer().writeValueAsString(result)
        .replaceAll(findIgnored, replaceIgnored);
    assertEquals(expectedString, resultString);
  }

  private WorkerWebUILogs getWebUILogsData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.WEBUI_LOGS),
            NO_PARAMS, HttpMethod.GET, null).call();
    WorkerWebUILogs data = new ObjectMapper().readValue(result, WorkerWebUILogs.class);
    return data;
  }

  @Test
  public void WebUILogs() throws Exception {
    WorkerWebUILogs result = getWebUILogsData();
    String expectedJson =
        ("{`currentPath`:``, `debug`:false, `fatalError`:null, `fileData`:null, `fileInfos`:[], "
            + "`invalidPathError`:``, `ntotalFile`:0, `viewingOffset`:0}").replace('`', '"');
    WorkerWebUILogs expected = new ObjectMapper().readValue(expectedJson, WorkerWebUILogs.class);
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
    String resultString = new ObjectMapper().writer().writeValueAsString(result);
    assertEquals(expectedString, resultString);
  }
}
